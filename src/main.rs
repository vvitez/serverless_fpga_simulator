//use core::error;
use std::clone;
use std::io::{BufRead, StdoutLock};

use std::net::{Ipv4Addr, SocketAddrV4};

use std::collections::VecDeque;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::ReuniteError;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use tokio::time::{sleep, timeout, Duration};

use lazy_static::*;
use std::collections::HashMap;
use std::io::Error;
use std::net::SocketAddr;
use std::sync::RwLock;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use byteorder::*;

const HEADER_BYTE: u8 = 0x80; //1000 0000
const DATA_BYTE: u8 = 0x40; //0100 0000
const STOP_BYTE: u8 = 0x08; //0000 1000
const ERR_BYTE: u8 = 0x07; //0000 0111
const RESPONSE_BYTE: u8 = 0x20; //0010 0000
const FOOTER_BYTE: u8 = 0x10; //0001 0000

const PACKET_SIZE: usize = 1024;
const RESPONSE_DATA_BUF_SIZE: usize = PACKET_SIZE - 2; //also response error buf size
const RESPONSE_FOOTER_BUF_SIZE: usize = PACKET_SIZE - 4;
const REQUEST_HEADER_BUF_SIZE: usize = PACKET_SIZE - 8;
const REQUEST_DATA_BUF_SIZE: usize = PACKET_SIZE - 2;

const TILES: usize = 4;

/*
The way I want this to work is that since we need to be able to accept different connections and work with them,
I will spawn a number of request handlers.
These get the request, serialize it into a request message and then pass it to a function handling
what happens next. This message includes a tile id. This is very important!
From now on we will stop using the weird lazystatic stuff, and just pass an entire fpga_state struct reference
to everybody. This struct should contain a list of arc-mutexed tile structs.
These tile structs need the following things to work:
this thing should be able to receive for the most current non-executing invocation.
I will make the simplification, that the currently executing (fully received) invocation will be
popped out of the invocation queue in each tile, executed, and sent back fully, such that there can't be anything
stopping it.
We still need a queue in case we manage to receive more than one entire invocation before the current one is done
executing.
This will be a problem, because how do we know an executing tile is in flight?
We need to wait until the first one is done to act like the real fpga.

for now, let's just use a mutex lock that we lock when we start executing and keep in scope until we are done.



*/

#[derive(Debug, Clone)]
struct Invocation {
    bitstream_id: u16,
    dataleft: usize,
    totalsize: usize,
    input: Vec<u8>, //only the data bit of the messages, concatinated
}

//the state per tile
#[derive(Debug, Clone)]
struct Tile {
    //shouldn't need tile_id here
    pending_invocations: Arc<Mutex<VecDeque<Arc<Mutex<Invocation>>>>>, //so we need to lock twice
}

#[derive(Debug, Clone)]
struct Fpga {
    //any more state to hold?
    tiles: [Tile; TILES],
}

/*
flag byte (for now independent of sending or receiving)
isHeader    isData      isResponse      isResponseFooter

Stop        ErrC        ErrC                isErr
*/
#[derive(Debug, Clone, Copy)]
struct ResponseFooter {
    flag_byte: u8,  //1
    tile_id: u8,    //1
    data_size: u16, //2  //data of this LAST packet only.
    //should be enough since we only need to know how much of the data left is inside the received buffer.
    data: [u8; RESPONSE_FOOTER_BUF_SIZE], //WILL be used now
}
#[derive(Debug, Clone, Copy)]
struct ResponseData {
    flag_byte: u8, //1
    tile_id: u8,   //1
    data: [u8; RESPONSE_DATA_BUF_SIZE],
}
#[derive(Debug, Clone, Copy)]
struct ResponseError {
    //error has no use for data
    flag_byte: u8, //1
    tile_id: u8,   //1
                   //padding: [u8; RESPONSE_DATA_BUF_SIZE],
}

#[derive(Debug, Clone, Copy)]
enum ResponseMessage {
    Data(ResponseData),
    Footer(ResponseFooter),
    Error(ResponseError),
}

fn serialize_response_message(msg: ResponseMessage) -> [u8; PACKET_SIZE] {
    let mut result: [u8; PACKET_SIZE] = [0; PACKET_SIZE];

    match msg {
        ResponseMessage::Footer(footer) => {
            result[0] = footer.flag_byte;
            result[1] = footer.tile_id;
            result[2..4].copy_from_slice(&footer.data_size.to_le_bytes());
            result[4..].copy_from_slice(&footer.data)
        }
        ResponseMessage::Data(data) => {
            result[0] = data.flag_byte;
            result[1] = data.tile_id;
            result[2..].copy_from_slice(&data.data)
        }
        ResponseMessage::Error(error) => {
            result[0] = error.flag_byte;
            result[1] = error.tile_id;
            //result[2..] is already zeroed out.
        }
    }

    result
}

#[derive(Debug, Clone, Copy)]
struct RequestHeader {
    flag_byte: u8,                       //1
    tile_id: u8,                         //1
    bitstream_id: u16,                   //2
    data_size: u32,                      //4
    data: [u8; REQUEST_HEADER_BUF_SIZE], //WILL BE USED NOW
}

#[derive(Debug, Clone, Copy)]
struct RequestData {
    flag_byte: u8, //1
    tile_id: u8,   //1
    data: [u8; REQUEST_DATA_BUF_SIZE],
}
#[derive(Debug, Clone, Copy)]
enum RequestMessage {
    Header(RequestHeader),
    Data(RequestData),
}

fn deserialize_request_message(buf: &[u8; PACKET_SIZE]) -> RequestMessage {
    let flag_byte = buf[0];
    let is_header = flag_byte & HEADER_BYTE != 0u8; //first bit == is_header
    if (is_header) {
        let mut data: [u8; REQUEST_HEADER_BUF_SIZE] = [0; REQUEST_HEADER_BUF_SIZE];
        data.copy_from_slice(&buf[8..PACKET_SIZE]);
        let header = RequestHeader {
            flag_byte,
            tile_id: buf[1],
            bitstream_id: u16::from_le_bytes([buf[2], buf[3]]),
            data_size: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            data,
        };
        RequestMessage::Header(header)
    } else {
        let mut data: [u8; REQUEST_DATA_BUF_SIZE] = [0; REQUEST_DATA_BUF_SIZE];
        data.copy_from_slice(&buf[2..PACKET_SIZE]);
        let data = RequestData {
            flag_byte,
            tile_id: buf[1],
            data,
        };
        RequestMessage::Data(data)
    }
}

//takes a finished buffer and sends it out every time from the receiver
pub async fn sender(
    mut rx: Receiver<Box<[u8; PACKET_SIZE]>>,
    remote_addr: SocketAddrV4,
) -> Result<(), Error> {
    loop {
        match TcpStream::connect(remote_addr).await {
            Ok(mut socket) => {
                while let Some(packet) = rx.recv().await {
                    if let Err(e) = socket.write_all(&packet[..]).await {
                        eprintln!("Write error: {e}");
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("Connect error: {e}");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/*
//gets a buffer, parses it and adds it to the correct invocation if it exists, otherwise creates it if it's a header
async fn handle_input_buffer(
    buf: &[u8; 1024],
    socket: &mut TcpStream,
    fpga_state: Arc<Fpga>,
) -> Result<bool, String> {
    let msg = deserialize_request_message(buf);

    match msg {
        RequestMessage::Header(header) => {
            let tile_id: usize = usize::from(header.tile_id);
            println!("new invocation found! tile_id: {}", tile_id);
            assert!(tile_id < TILES); //check id in bounds

            //get the tile state
            let curr_tile = &fpga_state.tiles[tile_id];

            //get the queue lock
            let mut pending_invocations = curr_tile.pending_invocations.lock().await;
            //TODO: make this resilient in the future.

            //check the front one, that should be the last one
            let opt_back_invocation = pending_invocations.back();
            if let Some(lock_back_invocation) = opt_back_invocation {
                let back_invocation = lock_back_invocation.lock().await;
                if back_invocation.state == InvocationState::WaitingForData {
                    // we got a new invocation but the current one isnt yet done receiving data
                    //TODO: ultimately i should return an error here.
                    eprintln!(
                        "got new invocation for tile even though the tile has unfinished receive"
                    );
                    return Err(
                        "got new header, but there is an unfinished receive invocation".to_string(),
                    );
                }
            }
            //so here we create a new one, plonk it in at the end
            let curr_invocation = Invocation {
                bitstream_id: header.bitstream_id,
                state: InvocationState::WaitingForData,
                messages: Vec::new(),
                dataleft: header.data_size as usize, //didnt subtract the current message yet
                input: Vec::new(),
            };
            //insert in queue and then work off of that.
            pending_invocations.push_back(Arc::new(Mutex::new(curr_invocation)));
            //now get the thing we did and fill it
            let opt_back_invocation = pending_invocations.back();
            match opt_back_invocation {
                None => {
                    //what the f*** is going on
                    eprintln!("this should never ever happen");
                    return Err("somehow the thing we just put into the queue is gone".to_string());
                }
                Some(lock_back_invocation) => {
                    println!("{:?}", header); //TODO: remove
                    let mut back_invocation = lock_back_invocation.lock().await;
                    //now we handle it like a data packet
                    back_invocation.messages.push(msg);
                    if (header.data_size as usize <= header.data.len()) {
                        //data fits inside the header
                        back_invocation.dataleft = 0;
                        back_invocation.input.extend_from_slice(
                            &header.data[0..header.data_size.try_into().unwrap()],
                        );
                        back_invocation.state = InvocationState::Pending;
                        //TODO: call and await the executor function, then send

                        //clone the arc pointer
                        let cloned_lock_back_invocation = Arc::clone(lock_back_invocation);

                    //dont drop the cloned_back_invocation though

                    //----------------------------------------------------------------------------------------------------------------------------------
                    } else {
                        back_invocation.dataleft -= header.data.len();
                        back_invocation.input.extend_from_slice(&header.data);
                        return Ok(true); //stuff went right.
                    }
                    return Err("NYI".to_string());
                }
            }
        }
        RequestMessage::Data(data) => {
            let tile_id: usize = usize::from(data.tile_id);

            println!("new data found! id: {}", tile_id);
            assert!(tile_id < TILES); //check id in bounds

            //get the tile state
            let curr_tile = &fpga_state.tiles[tile_id];

            //get the queue lock
            let mut pending_invocations = curr_tile.pending_invocations.lock().await;
            //TODO: make this resilient in the future.
            let opt_back_invocation = pending_invocations.back();
            match opt_back_invocation {
                None => {
                    eprintln!("got data but theres no invocation");
                    return Err("got data but theres no invocation in the tile queue".to_string());
                }
                Some(lock_back_invocation) => {
                    let mut back_invocation = lock_back_invocation.lock().await;
                    //check state
                    if (back_invocation.state != InvocationState::WaitingForData) {
                        eprintln!("the current invocation is already pending or done!");
                        return Err(
                            "somehow we got a data packet but no header was sent yet".to_string()
                        );
                    }
                    back_invocation.messages.push(msg);
                    //here we should be fine to just load stuff in
                    if (back_invocation.dataleft <= data.data.len()) {
                        let dataleft = back_invocation.dataleft;

                        back_invocation
                            .input
                            .extend_from_slice(&data.data[0..dataleft]);
                        back_invocation.state = InvocationState::Pending;
                        back_invocation.dataleft = 0;
                        //TODO: call and await execute function
                    } else {
                        back_invocation.dataleft -= data.data.len();
                        back_invocation.input.extend_from_slice(&data.data);
                        return Ok(true); //all went well
                    }

                    return Err("NYI".to_string());
                }
            }
        }
    }
}
*/

async fn receiver(threadnum: u32, listener: Arc<Mutex<TcpListener>>, fpga_state: Arc<Fpga>) {
    println!("t{} start", threadnum);
    loop {
        //remove outer loop for now, so that we can see the different threads working

        let (mut socket, _) = listener.lock().await.accept().await.unwrap();

        println!("t{}: accepted new connection!", threadnum);
        let mut buffer: [u8; PACKET_SIZE] = [0; PACKET_SIZE];
        loop {
            //there is an async read stuff in tokio poll_read
            match socket.readable().await {
                Ok(()) => {
                    let n = match timeout(Duration::from_secs(2), socket.read(&mut buffer)).await {
                        Ok(Ok(PACKET_SIZE)) => PACKET_SIZE,
                        Ok(Ok(0)) => {
                            sleep(Duration::from_millis(10)).await;
                            continue; //TODO: VICTOR find a better way to do this, other than "busy" polling and sleeping
                        }

                        Ok(Ok(n)) => {
                            eprintln!("did not get packet size, got {n}");
                            break;
                        }

                        Ok(Err(e)) => {
                            eprintln!("got error in read: {e}");
                            break;
                        }
                        Err(_) => {
                            eprintln!("got timeout!");
                            break;
                        }
                    };
                    println!("t{}: Size received: {} bytes", threadnum, n);
                    let trunc_received_data = &buffer[..20];
                    let trunc_received_chars: Vec<char> = trunc_received_data
                        .iter()
                        .map(|&byte| byte as char)
                        .collect();
                    println!("t{}: Received: {:?}", threadnum, trunc_received_chars);
                    //let result =
                    //    handle_input_buffer(&buffer, &mut socket, fpga_state.clone()).await;
                    //println!("{:?}", result);
                }
                Err(err) => {
                    eprintln!("failed to get readable: {err}");
                    break;
                }
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn receive_worker(fpga_state: Arc<Fpga>, listen_conn: SocketAddrV4, send_conn: SocketAddrV4) {
    let listener = TcpListener::bind(listen_conn)
        .await
        .expect("failed to bind recv_connection");

    //start sender
    let (tx, mut rx) = mpsc::channel::<Box<[u8; PACKET_SIZE]>>(10);
    tokio::spawn(sender(rx, send_conn));
    //start tileworkers and connect to sender
    for i in 0..4 {
        tokio::spawn(tile_worker(i, fpga_state.clone(), tx.clone()));
    }
    //big loop reistablishing connection
    loop {
        let (mut socket, _) = listener
            .accept()
            .await
            .expect("failed to accept connection");
        println!("accepted connection!!");
        //inner loop reading packets
        loop {
            let mut buffer: Box<[u8; PACKET_SIZE]> = Box::new([0; PACKET_SIZE]);

            match socket.readable().await {
                Err(err) => {
                    eprintln!("failed to get readable: {err}.\n Re-establishing connection...");
                    break;
                }
                Ok(()) => {
                    let _n = match timeout(Duration::from_secs(2), socket.read(&mut *buffer)).await
                    {
                        Ok(Ok(PACKET_SIZE)) => PACKET_SIZE,
                        Ok(Ok(0)) => {
                            sleep(Duration::from_millis(50)).await;
                            //println!("got empty packet");
                            break; //TODO: find a better way to do this, other than "busy" polling and sleeping
                        }

                        Ok(Ok(n)) => {
                            eprintln!("did not get packet size, got {n}");
                            panic!("something is wrong, I can feel it... read size");
                            //break;
                        }

                        Ok(Err(e)) => {
                            eprintln!("got error in read: {e}");
                            panic!("something is wrong, I can feel it... read err");
                            //break;
                        }

                        Err(_) => {
                            eprintln!("got timeout! reconnecting...");
                            break;
                        }
                    };
                    println!("got a packet");
                    let packet_tile_id: u8 = buffer[1];
                    let tile_id = usize::from(packet_tile_id);
                    if usize::from(packet_tile_id) >= 4 {
                        panic!("got nonsensical tile id {:?}", packet_tile_id);
                    }
                    //read out, deserialize, lock invocation and write data to its vec
                    //on every new header, build an invocation and put it into the queue

                    match deserialize_request_message(&buffer) {
                        RequestMessage::Header(request_header) => {
                            let bitstream_id = request_header.bitstream_id;
                            let data_size: usize =
                                request_header.data_size.try_into().expect("idk man");
                            //build invocation
                            let mut input: Vec<u8> = Vec::new();
                            println!("full data length:  {data_size:?}");
                            input.extend_from_slice(&request_header.data);
                            let dataleft = if (data_size >= REQUEST_HEADER_BUF_SIZE) {
                                data_size - REQUEST_HEADER_BUF_SIZE
                            } else {
                                0
                            };
                            let mut invocation = Invocation {
                                bitstream_id,
                                dataleft,
                                totalsize: data_size,
                                input,
                            };

                            //put on queue
                            println!("putting header on queue");
                            {
                                let mut queue =
                                    fpga_state.tiles[tile_id].pending_invocations.lock().await;
                                queue.push_back(Arc::new(Mutex::new(invocation)));
                            }
                        }
                        RequestMessage::Data(request_data) => {
                            //fetch last one in tile queue
                            let invocation = {
                                let mut queue =
                                    fpga_state.tiles[tile_id].pending_invocations.lock().await;
                                let invocation =
                                    queue.back().expect("got data but nothing in queue");
                                invocation.clone()
                            };
                            println!("got data packet, updating invocation..");
                            {
                                let mut locked_invocation = invocation.lock().await;
                                locked_invocation
                                    .input
                                    .extend_from_slice(&request_data.data);

                                let sub_res = locked_invocation
                                    .dataleft
                                    .checked_sub(REQUEST_DATA_BUF_SIZE);
                                if let Some(val) = sub_res {
                                    locked_invocation.dataleft = val;
                                } else {
                                    locked_invocation.dataleft = 0;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn tile_worker(
    tile_id: usize,
    fpga_state: Arc<Fpga>,
    sender: Sender<Box<[u8; PACKET_SIZE]>>,
) {
    //periodically sleep and check tile queue for invocations
    loop {
        let (qlen) = {
            let queue = fpga_state.tiles[tile_id].pending_invocations.lock().await;
            let qlen = queue.len();

            qlen
        };

        if (qlen == 0) {
            sleep(Duration::from_millis(2)).await;
            continue; //try again later, adds nice latency
        }
        println!("found invocation on queue!");

        //if we are that means we have an item on the queue
        let (invocation) = {
            let queue = fpga_state.tiles[tile_id].pending_invocations.lock().await;
            let front_invocation = queue.front().expect("i give up").clone();
            front_invocation
        };
        //we want to keep that until we're done with it, by which time the outer loop executes again

        let mut sent_so_far: usize = 0;

        let (bitstream_id, mut total_len, mut dataleft) = {
            let locked_invocation = invocation.lock().await;
            let bitstream_id = locked_invocation.bitstream_id;
            let total_len = locked_invocation.totalsize;
            let dataleft = locked_invocation.dataleft;

            (bitstream_id, total_len, dataleft)
        };

        loop {
            println!("tileworker {tile_id:?} inner loop");
            if (dataleft == 0 && (total_len - sent_so_far) <= RESPONSE_FOOTER_BUF_SIZE) {
                println!("footer_case: total_len {total_len:?}  sent_so_far {sent_so_far:?}");
                //if done and rest/everything fits in one footer, execute and send that
                //potentially zero-extend until it fits the size
                let mut computed_result = {
                    let locked_invocation = invocation.lock().await;
                    bitstream_id_adder(
                        &locked_invocation.input[sent_so_far..],
                        total_len - sent_so_far,
                        bitstream_id,
                    )
                };

                let real_last_packet_len = computed_result.len();
                let mut data = [0u8; RESPONSE_FOOTER_BUF_SIZE];
                data[..real_last_packet_len]
                    .copy_from_slice(&computed_result[..real_last_packet_len]);
                //build footer
                let footer = ResponseFooter {
                    flag_byte: FOOTER_BYTE,
                    tile_id: tile_id.try_into().expect("idk"),
                    data_size: real_last_packet_len.try_into().expect("idk"),
                    data,
                };
                let packet = serialize_response_message(ResponseMessage::Footer(footer));
                sent_so_far += real_last_packet_len;
                sender
                    .send(Box::new(packet))
                    .await
                    .expect("failed to send to sender thread");

                //yeet invocation from queue
                {
                    let mut queue = fpga_state.tiles[tile_id].pending_invocations.lock().await;
                    queue.pop_front();
                }

                break; //we are done here
            } else if (total_len - dataleft - sent_so_far >= REQUEST_DATA_BUF_SIZE) {
                //can process and send a data packet

                let mut computed_result = {
                    let locked_invocation = invocation.lock().await;
                    bitstream_id_adder(
                        &locked_invocation.input[sent_so_far..sent_so_far + REQUEST_DATA_BUF_SIZE],
                        REQUEST_DATA_BUF_SIZE,
                        bitstream_id,
                    )
                };

                let mut data = [0u8; RESPONSE_DATA_BUF_SIZE];
                data[..].copy_from_slice(&computed_result[..]);
                let dataresponse = ResponseData {
                    flag_byte: RESPONSE_BYTE,
                    tile_id: tile_id.try_into().expect("idk"),
                    data,
                };
                let packet = serialize_response_message(ResponseMessage::Data(dataresponse));
                sent_so_far += REQUEST_DATA_BUF_SIZE;
                sender
                    .send(Box::new(packet))
                    .await
                    .expect("failed to send to sender thread");
            }

            sleep(Duration::from_millis(2)).await;
            (dataleft) = {
                let locked_invocation = invocation.lock().await;
                locked_invocation.dataleft
            }; //update
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let tiles: [Tile; TILES] = core::array::from_fn(|_| Tile {
        pending_invocations: Arc::new(Mutex::new(VecDeque::new())),
    });

    let fpga_state: Arc<Fpga> = Arc::new(Fpga { tiles });

    let listen_conn: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3456);
    let send_conn = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3457);

    receive_worker(fpga_state, listen_conn, send_conn).await;
}

//take data as 32 bit ints and add on the bitstream_id
fn bitstream_id_adder(input: &[u8], datasize: usize, bitstream_id: u16) -> Vec<u8> {
    assert!(!input.is_empty());

    println!("datasize = {datasize:?}");

    assert!(datasize % 2 == 0);
    println!("adder called!");
    let size = datasize / 2;
    let i16_bitstream_id: i16 = bitstream_id.try_into().expect("msg");
    let mut converted_input: Vec<i16> = vec![0; size];
    LittleEndian::read_i16_into(&input[..datasize], &mut converted_input);
    let mut output: Vec<i16> = vec![0; size];
    println!("adder got size: {size:?}");
    for i in 0..size {
        output[i] = (converted_input[i] + i16_bitstream_id);
    }
    let mut result: Vec<u8> = vec![0; datasize];
    LittleEndian::write_i16_into(&output, &mut result);

    return result;
}

/*
 * Plans:
 * split handling a request into two threads, one, that calculates the result (message by message if needed)
 * and saves in a big double ended queue (because it is built on a ring buffer)
 *
 * and a thread that iterates through the result and sends a packet whenever enough data is ready.
 *
 * Every function invocation gets its own handler now. Matrix multiplication obviously doesn't run stream,
 * and instead "needs" all of the data to start calculating the response. (well, at least most of the data.)
 *
 * The function invocation handler decides when to start any of the two threads that calculate result and send it out.
 * For a streaming task, we should be generally able to start calculating the result after the first message in a
 * multi-message stream.
 *
 *
 * problem: a response data message can contain more data than a response footer message.
 * Theoretically, you can end up with a full or even non-full response data message followed by an empty
 * footer, indicating that the total data size is smaller than the previous message's data part, meaning that part of
 * it was zero-padded.
 *
 * We can either handle this here and in dandelion (not a big problem)
 * or we just set the result message data portions to be the same size for the data and footer message types.
 *
 */

/*
i want to have one receiving node that plops the data where it belongs and notifies the tile FSMs (which
calculate and send responses), then check if they can continue calculating, and do so if possible.

 */
