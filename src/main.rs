/* main.rs

   Testing Ground as of now

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2024
*/

use fusion_framework::datastore::{build_graph_integer_data, DataStore};
use fusion_framework::rpc::{DataType, SessionHeader, RPC};
use fusion_framework::udf::{GraphSum, NMASInfo, NaiveMaxAdjacentSum};
use fusion_framework::vertex::MachineID;
use fusion_framework::worker::Worker;
use fusion_framework::UserDefinedFunction;

use hashbrown::{HashMap, HashSet};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use uuid::Uuid;

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
            rpc_receiving_streams.insert(2, rpc_receiving_stream);
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
            rpc_receiving_streams.insert(1, rpc_receiving_stream);
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

    // constructing graph for multi-thread sharing
    let worker = Arc::new(worker);
    let mut data_store = DataStore::default();

    // use graph builder to build the graph based on machine_id
    build_graph_integer_data(&mut data_store, machine_id, worker.clone());
    println!("Graph built for testing");

    let data_store = Arc::new(data_store);

    // getting fixed size for reception
    let dummy_rpc = RPC::Execute(Uuid::default(), 0, 0);
    let dummy_rpc_len = bincode::serialize(&dummy_rpc).unwrap().len();
    // handle rpc receiving streams
    for (id, stream) in rpc_receiving_streams.into_iter() {
        let worker = worker.clone();
        let data_store = data_store.clone();
        tokio::spawn(async move {
            // handle_rpc_receiving_stream(&id, stream, worker, data_store, &GraphSum, dummy_rpc_len)
            //     .await;
            handle_rpc_receiving_stream(
                &id,
                stream,
                worker,
                data_store,
                &NaiveMaxAdjacentSum,
                dummy_rpc_len,
            )
            .await;
        });
    }

    // getting fixed size for reception
    let dummy_session_control_data_len = bincode::serialize(&SessionHeader {
        session_id: Uuid::default(),
        session_type: DataType::Result,
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

    // Note: For testing, invoke functions on machine 1
    if machine_id == 1 {
        // Apply function and print result
        let root = data_store.get_vertex_by_id(&0);
        let distance = 2;
        // let result = root.apply_function(&GraphSum, &data_store, None).await;
        let result = root
            .apply_function(
                &NaiveMaxAdjacentSum,
                &data_store,
                Some(NMASInfo {
                    source: None,
                    distance,
                    started: Some(HashSet::new()),
                }),
            )
            .await;
        // let result = root.apply_function(&GraphSum, &data_store, None).await;
        // println!("The graph sum is: {result}");
        println!("The Max Adjacent Sum for {distance} is: {result}");
    }

    // keeping the machine running
    loop {}
}

async fn handle_data_receiving_stream<T: Serialize + DeserializeOwned, V: DeserializeOwned>(
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
        let session_header = bincode::deserialize::<SessionHeader>(&session_header_bytes).unwrap();

        // depending the type, do different things
        match &session_header.session_type {
            DataType::NotYetNeeded => {
                unimplemented!()
            }
            DataType::Result => {
                // get where the channel the result should go to
                let res_channels = worker.result_multiplexing_channels.read().await;
                let res_channel = res_channels
                    .get(&session_header.session_id)
                    .expect("session id not exist in aux_info channel")
                    .lock()
                    .await;

                // construct the data buffer and receive them
                let mut res_bytes = vec![0u8; session_header.data_len];
                data_receiving_stream
                    .read_exact(&mut res_bytes)
                    .await
                    .unwrap();

                // send over the result
                let res = bincode::deserialize::<V>(&res_bytes).unwrap();
                res_channel.send(res).await.unwrap();
            }
        }
    }
}

async fn handle_rpc_receiving_stream<
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    U: Serialize + DeserializeOwned + Send + Sync + 'static,
    X: UserDefinedFunction<T, U, V> + Send + Sync + 'static + Clone,
    V: Serialize + Send + Sync + 'static,
>(
    id: &MachineID,
    mut stream: TcpStream,
    worker: Arc<Worker<T, V>>,
    data_store: Arc<DataStore<T, V>>,
    _type: &X,
    dummy_rpc_len: usize,
) {
    // construct the buffer to receive fixed size of bytes for RPC
    let mut cmd = vec![0u8; dummy_rpc_len];

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
                    // calculate the result non-blocking-ly, without holding onto locks prior to entrance
                    let res = data_store_clone
                        .get_vertex_by_id(&v_id)
                        .apply_function(&_type_clone, &data_store_clone, aux_info)
                        .await;

                    // get sending_stream as mut
                    let sending_streams = worker_clone.sending_streams.read().await;
                    let mut sending_stream = sending_streams.get(&id_clone).unwrap().lock().await;

                    let res_bytes = bincode::serialize::<V>(&res).unwrap();

                    // construct session header
                    let session_header_for_result = SessionHeader {
                        session_id: uuid,
                        session_type: DataType::Result,
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
            RPC::Update(_, _, _) => {
                unimplemented!()
            }
        }
    }
}
