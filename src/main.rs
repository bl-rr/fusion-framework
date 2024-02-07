/* main.rs

   Testing Ground as of now

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2024
*/

use core::fmt::Debug;

use fusion_framework::datastore::{build_graph_integer_data, DataStore};
use fusion_framework::rpc::{RPCResPayload, RPCResponseHeader, ResType, RPC};
use fusion_framework::udf::{GraphSum, NMASInfo, NaiveMaxAdjacentSum, SwapLargestAndSmallest};
use fusion_framework::vertex::{Data, MachineID, VertexID};
use fusion_framework::worker::Worker;
use fusion_framework::UserDefinedFunction;

use hashbrown::{HashMap, HashSet};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

async fn handle_data_receiving_stream<
    T: Serialize + DeserializeOwned + Default,
    V: DeserializeOwned + Default + Debug,
>(
    mut data_receiving_stream: TcpStream,
    worker: Arc<Worker<T, V>>,
    dummy_session_control_data_len: usize,
) {
    // construct the reception buffer for session_header
    let mut session_header_bytes = vec![0u8; dummy_session_control_data_len];

    // each time, start with receiving the header
    while let Ok(_) = data_receiving_stream
        .read_exact(&mut session_header_bytes)
        .await
    {
        let session_header =
            bincode::deserialize::<RPCResponseHeader>(&session_header_bytes).unwrap();

        // depending on the type, do different things
        match &session_header.session_type {
            ResType::NotYetNeeded => {
                unimplemented!()
            }
            ResType::ExecuteRes | ResType::UpdateRes => {
                // get where the channel the result should go to
                let res_channels = worker.result_multiplexing_channels.read().await;
                let res_channel = res_channels
                    .get(&session_header.session_id)
                    .expect("session id not exist in result channel")
                    .lock()
                    .await;

                // construct the data buffer and receive them
                let mut res_bytes = vec![0u8; session_header.data_len];
                data_receiving_stream
                    .read_exact(&mut res_bytes)
                    .await
                    .unwrap();

                // send over the result
                let res = bincode::deserialize::<RPCResPayload<_, _>>(&res_bytes).unwrap();
                res_channel.send(res).await.unwrap();
            }
        }
    }
}

async fn handle_rpc_receiving_stream<
    T: Serialize + DeserializeOwned + Send + Sync + 'static + Debug + Default,
    U: Serialize + DeserializeOwned + Send + Sync + 'static,
    X: UserDefinedFunction<T, U, V> + Send + Sync + 'static + Clone,
    V: Serialize + Send + Sync + 'static + Debug,
>(
    id: Arc<MachineID>,
    stream: Arc<Mutex<TcpStream>>,
    worker: Arc<Worker<T, V>>,
    data_store: Arc<DataStore<T, V>>,
    _type: &X,
    dummy_rpc_len: usize,
    tx_req: Sender<MachineID>,
    tx_res: Sender<()>,
) {
    // construct the buffer to receive fixed size of bytes for RPC
    let mut cmd = vec![0u8; dummy_rpc_len];

    let mut stream = stream.lock().await;

    while let Ok(_) = stream.read_exact(&mut cmd).await {
        match bincode::deserialize::<RPC>(&cmd).expect("Incorrect RPC format") {
            RPC::Execute(uuid, v_id, aux_info_len) => {
                // construct the buffer for auxiliary information and receive it
                let mut aux_info = vec![0u8; aux_info_len];
                stream.read_exact(&mut aux_info).await.unwrap();
                // it comes in the same RPC stream as noted in remote_execute()

                let aux_info =
                    bincode::deserialize::<U>(&aux_info).expect("Incorrect Auxiliary Info Format");

                // construct variable to pass into the new thread, for non-blocking circular/recursive remote calls
                let worker_clone = worker.clone();
                let data_store_clone = data_store.clone();
                let _type_clone = _type.clone();
                let id_clone = id.clone();

                tokio::spawn(async move {
                    // calculate the result in a non-blocking manner, without holding onto locks prior to entrance
                    let res = data_store_clone
                        .get_vertex_by_id(&v_id)
                        .apply_function(&_type_clone, &data_store_clone, aux_info)
                        .await;

                    // construct result that is to be sent back
                    let res: RPCResPayload<T, V> = RPCResPayload::ExecuteResPayload(res);

                    // get sending_stream as mut
                    let sending_streams = worker_clone.sending_streams.read().await;
                    let mut sending_stream =
                        sending_streams.get(id_clone.as_ref()).unwrap().lock().await;

                    let res_bytes = bincode::serialize::<RPCResPayload<_, V>>(&res).unwrap();

                    // construct session header
                    let session_header_for_result = RPCResponseHeader {
                        session_id: uuid,
                        session_type: ResType::ExecuteRes,
                        data_len: res_bytes.len(),
                    };
                    let session_header_for_result_bytes =
                        bincode::serialize(&session_header_for_result).unwrap();

                    // send all the data
                    sending_stream
                        .write_all(&[session_header_for_result_bytes, res_bytes].concat())
                        .await
                        .unwrap();
                });
            }
            RPC::Relay(_, _, _) => {
                unimplemented!()
            }
            RPC::RequestData(_, _, _) => {
                unimplemented!()
            }
            RPC::ExecuteWithData(_, _, _) => {
                unimplemented!()
            }
            RPC::Update(uuid, v_id, data_len) => {
                // construct the buffer for data and receive it
                let mut data = vec![0u8; data_len];
                stream.read_exact(&mut data).await.unwrap();
                // it comes in the same RPC stream as noted in remote_execute()

                let data = bincode::deserialize::<Data<T>>(&data)
                    .expect("Incorrect Auxiliary Info Format");

                // Note: Different here, doesn't need to multi-thread here, as update is pure local (synchronous in
                // another sense

                let res = data_store.get_vertex_by_id(&v_id).update(data).await;

                // construct the payload to be sent
                let res: RPCResPayload<T, V> = RPCResPayload::UpdateResPayload(res);
                let res_bytes = bincode::serialize::<RPCResPayload<T, _>>(&res).unwrap();

                // get sending_stream as mut
                let sending_streams = worker.sending_streams.read().await;
                let mut sending_stream = sending_streams.get(id.as_ref()).unwrap().lock().await;

                // construct session header
                let session_header_for_result = RPCResponseHeader {
                    session_id: uuid,
                    session_type: ResType::UpdateRes,
                    data_len: res_bytes.len(),
                };

                let session_header_for_result_bytes =
                    bincode::serialize(&session_header_for_result).unwrap();

                // send all the data
                sending_stream
                    .write_all(&[session_header_for_result_bytes, res_bytes].concat())
                    .await
                    .unwrap();
            }
            RPC::UpdateMap(_, _, _) => {
                println!("received update request");
                tx_req.send(id.as_ref().clone()).await.unwrap();
            }
            RPC::UpdateMapRes(_, _, _) => {
                println!("received update resp");
                tx_res.send(()).await.unwrap()
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // a machine id needs to be provided
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <machine_id>", args[0]);
        std::process::exit(1);
    }
    let machine_id: MachineID = args[1].parse().expect("Invalid machine ID");
    let remote_machine_id: MachineID = if machine_id == 1 { 2 } else { 1 };

    // calculate self port and remote port based on input machine id
    let base_port = 8080;
    let local_port = base_port + machine_id;
    let remote_port = base_port + remote_machine_id;
    let local_address = format!("127.0.0.1:{}", local_port);
    let remote_address = format!("127.0.0.1:{}", remote_port);

    // streams for handling initial communication
    let mut rpc_receiving_streams = HashMap::new();
    let mut data_receiving_streams = vec![];

    // Start listening on the local port
    let listener = TcpListener::bind(&local_address)
        .await
        .expect("Failed to bind local address");
    println!("Listening on {}", local_address);

    // Create new worker instance
    let worker = Worker::new();
    // other communication channel
    let (tx_update_req, mut rx_update_req) = channel::<MachineID>(100);
    let (tx_update_res, mut rx_update_res) = channel::<()>(100);

    match machine_id {
        // the order of communication is crucial for initial setup
        1 => {
            // (1) listens first, and needs to be launched first
            let (incoming_stream, socket_addr) = listener
                .accept()
                .await
                .expect("Failed to accept connection");
            println!("New connection from {socket_addr}");

            let (rpc_receiving_stream, socket_addr) = listener
                .accept()
                .await
                .expect("Failed to accept connection");
            println!("New connection from {socket_addr}");

            // after accepting incoming connections, then initiate outgoing connections
            let outgoing_stream = TcpStream::connect(&remote_address)
                .await
                .expect(&*format!("Failed to connect to {remote_address}"));
            let rpc_sending_stream = TcpStream::connect(&remote_address)
                .await
                .expect(&*format!("Failed to connect to {remote_address}"));

            // fill in the data structures
            rpc_receiving_streams.insert(Arc::new(2), Arc::new(Mutex::new(rpc_receiving_stream)));
            data_receiving_streams.push(incoming_stream);
            worker
                .sending_streams
                .write()
                .await
                .insert(2, Mutex::new(outgoing_stream));
            worker
                .rpc_sending_streams
                .write()
                .await
                .insert(2, Mutex::new(rpc_sending_stream));
        }
        2 => {
            // (2) initiates outgoing connections first, needs to be launched second
            let outgoing_stream = TcpStream::connect(&remote_address)
                .await
                .expect(&*format!("Failed to connect to {remote_address}"));

            let rpc_sending_stream = TcpStream::connect(&remote_address)
                .await
                .expect(&*format!("Failed to connect to {remote_address}"));

            // after connecting, then listen
            let (incoming_stream, socket_addr) = listener
                .accept()
                .await
                .expect("Failed to accept connection");
            println!("New connection from {socket_addr}");

            let (rpc_receiving_stream, socket_addr) = listener
                .accept()
                .await
                .expect("Failed to accept connection");
            println!("New connection from {socket_addr}");

            // fill in the data structures
            rpc_receiving_streams.insert(Arc::new(1), Arc::new(Mutex::new(rpc_receiving_stream)));
            data_receiving_streams.push(incoming_stream);
            worker
                .sending_streams
                .write()
                .await
                .insert(1, Mutex::new(outgoing_stream));
            worker
                .rpc_sending_streams
                .write()
                .await
                .insert(1, Mutex::new(rpc_sending_stream));
        }
        _ => unimplemented!(),
    }

    println!("Simulation Set Up Complete: Communication channels");

    // constructing datastructures for multi-thread sharing
    let worker = Arc::new(worker);
    let mut data_store = DataStore::new();

    // use graph builder to build the graph based on machine_id
    build_graph_integer_data(&mut data_store, machine_id, worker.clone());
    println!("Graph built for testing");

    let mut data_store = Arc::new(data_store);

    // getting fixed size for reception
    let dummy_rpc = RPC::Execute(Uuid::default(), 0, 0);
    let dummy_rpc_len = bincode::serialize(&dummy_rpc).unwrap().len();

    let mut data_store_holders = vec![];

    start_receiving_rpcs(
        &mut rpc_receiving_streams,
        &worker,
        &mut data_store,
        dummy_rpc_len,
        &mut data_store_holders,
        tx_update_req.clone(),
        tx_update_res.clone(),
    );

    // getting fixed size for reception
    let dummy_session_control_data_len = bincode::serialize(&RPCResponseHeader {
        session_id: Uuid::default(),
        session_type: ResType::ExecuteRes,
        data_len: 0,
    })
    .unwrap()
    .len();
    // handle data receiving streams
    for data_receiving_stream in data_receiving_streams.into_iter() {
        let worker = worker.clone();
        tokio::spawn(handle_data_receiving_stream(
            data_receiving_stream,
            worker,
            dummy_session_control_data_len,
        ));
    }
    println!("BEFORE!\n{:?}\n\n", data_store);

    // Note: For testing, invoke functions on machine 1
    if machine_id == 1 {
        // Apply function and print result
        let root = data_store.get_vertex_by_id(&0);
        let _distance = 2;
        let result = root.apply_function(&GraphSum, &data_store, None).await;
        println!("[1]: The graph sum is: {result}");

        // let result = root
        //     .apply_function(
        //         &NaiveMaxAdjacentSum,
        //         &data_store,
        //         Some(NMASInfo {
        //             source: None,
        //             distance: _distance,
        //             started: Some(HashSet::new()),
        //         }),
        //     )
        //     .await;
        // println!("The Max Adjacent Sum for {distance} is: {result}");

        // let result = root
        //     .apply_function(&SwapLargestAndSmallest, &data_store, true)
        //     .await;
        // println!("the result to swap largest and smallest is: {:?}", result);

        update_global_data_store(
            &mut rpc_receiving_streams,
            &tx_update_req,
            &tx_update_res,
            &mut rx_update_res,
            &worker,
            &mut data_store,
            dummy_rpc_len,
            &mut data_store_holders,
        )
        .await;

        let root = data_store.get_vertex_by_id(&0);
        let _distance = 2;
        let result = root.apply_function(&GraphSum, &data_store, None).await;
        println!("[2]: The graph sum is: {result}");
    } else {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // keeping the machine running
    loop {
        handle_update_reqs(
            &mut rpc_receiving_streams,
            tx_update_req.clone(),
            &mut rx_update_req,
            tx_update_res.clone(),
            &worker,
            &mut data_store,
            dummy_rpc_len,
            &mut data_store_holders,
        )
        .await;
    }
}

async fn handle_update_reqs(
    mut rpc_receiving_streams: &mut HashMap<Arc<MachineID>, Arc<Mutex<TcpStream>>>,
    tx_update_req: Sender<MachineID>,
    rx_update_req: &mut Receiver<MachineID>,
    tx_update_res: Sender<()>,
    worker: &Arc<Worker<isize, isize>>,
    mut data_store: &mut Arc<DataStore<isize, isize>>,
    dummy_rpc_len: usize,
    mut data_store_holders: &mut Vec<JoinHandle<()>>,
) {
    if let Some(source_id) = rx_update_req.recv().await {
        update_data_store(
            &mut rpc_receiving_streams,
            &worker,
            &mut data_store,
            dummy_rpc_len,
            &mut data_store_holders,
            tx_update_req.clone(),
            tx_update_res.clone(),
        )
        .await;

        let update_resp =
            RPC::UpdateMapRes(Default::default(), Default::default(), Default::default());
        let update_resp_bytes = bincode::serialize(&update_resp).unwrap();

        let rpc_sending_streams = worker.rpc_sending_streams.read().await;
        let mut stream = rpc_sending_streams.get(&source_id).unwrap().lock().await;
        stream.write_all(&update_resp_bytes).await.unwrap();

        println!("AFTER~\n{:?}", data_store);
    }
}

async fn update_global_data_store(
    mut rpc_receiving_streams: &mut HashMap<Arc<MachineID>, Arc<Mutex<TcpStream>>>,
    tx_update_req: &Sender<MachineID>,
    tx_update_res: &Sender<()>,
    rx_update_res: &mut Receiver<()>,
    worker: &Arc<Worker<isize, isize>>,
    mut data_store: &mut Arc<DataStore<isize, isize>>,
    dummy_rpc_len: usize,
    mut data_store_holders: &mut Vec<JoinHandle<()>>,
) {
    update_data_store(
        &mut rpc_receiving_streams,
        &worker,
        &mut data_store,
        dummy_rpc_len,
        &mut data_store_holders,
        tx_update_req.clone(),
        tx_update_res.clone(),
    )
    .await;
    // need all others to update as well
    let update_command = RPC::UpdateMap(Default::default(), Default::default(), Default::default());
    let update_command_bytes = bincode::serialize(&update_command).unwrap();

    let rpc_sending_streams = worker.rpc_sending_streams.read().await;
    for (_, stream) in rpc_sending_streams.iter() {
        let mut stream = stream.lock().await;
        stream.write_all(&update_command_bytes).await.unwrap();
    }

    // Wait for response
    for _ in 1..=rpc_sending_streams.len() {
        rx_update_res.recv().await.unwrap();
        println!("received");
    }
}

async fn update_data_store(
    mut rpc_receiving_streams: &mut HashMap<Arc<MachineID>, Arc<Mutex<TcpStream>>>,
    worker: &Arc<Worker<isize, isize>>,
    mut data_store: &mut Arc<DataStore<isize, isize>>,
    dummy_rpc_len: usize,
    mut data_store_holders: &mut Vec<JoinHandle<()>>,
    tx_req: Sender<MachineID>,
    tx_res: Sender<()>,
) {
    // kill all data_store_holders
    for stream in data_store_holders.drain(..) {
        stream.abort();
        stream.await.unwrap_or_default();
    }
    {
        println!("strong counts: {:?}", Arc::strong_count(&data_store));
        let data_store_mut = Arc::get_mut(&mut data_store).unwrap();
        data_store_mut.update().await;
    }
    start_receiving_rpcs(
        &mut rpc_receiving_streams,
        &worker,
        &mut data_store,
        dummy_rpc_len,
        &mut data_store_holders,
        tx_req,
        tx_res,
    );
}

fn start_receiving_rpcs(
    rpc_receiving_streams: &mut HashMap<Arc<MachineID>, Arc<Mutex<TcpStream>>>,
    worker: &Arc<Worker<isize, isize>>,
    data_store: &mut Arc<DataStore<isize, isize>>,
    dummy_rpc_len: usize,
    data_store_holders: &mut Vec<JoinHandle<()>>,
    tx_req: Sender<MachineID>,
    tx_res: Sender<()>,
) {
    // handle rpc receiving streams
    for (id, stream) in rpc_receiving_streams.iter() {
        let worker = worker.clone();
        let data_store = data_store.clone();
        let stream = stream.clone();
        let id = id.clone();
        let tx_req = tx_req.clone();
        let tx_res = tx_res.clone();

        let handle = tokio::spawn(async move {
            handle_rpc_receiving_stream(
                id,
                stream,
                worker,
                data_store,
                &GraphSum,
                dummy_rpc_len,
                tx_req,
                tx_res,
            )
            .await;

            // handle_rpc_receiving_stream(
            //     id,
            //     stream,
            //     worker,
            //     data_store,
            //     &NaiveMaxAdjacentSum,
            //     dummy_rpc_len,
            // )
            // .await;

            //     handle_rpc_receiving_stream(
            //         id,
            //         stream,
            //         worker,
            //         data_store,
            //         &SwapLargestAndSmallest,
            //         dummy_rpc_len,
            //     )
            //     .await;
        });

        data_store_holders.push(handle);
    }
}
