/* main.rs
   Testing Ground as of now

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2023
*/

use fusion_framework::graph::{build_graph_integer_data, Graph};
use fusion_framework::rpc::{DataType, SessionControl, RPC};
use fusion_framework::udf::{Direction, GraphSum, NMASInfo, NaiveMaxAdjacentSum};
use fusion_framework::vertex::MachineID;

use fusion_framework::UserDefinedFunction;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
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
    let mut data_receiving_streams = HashMap::new();

    // Start listening on the local port
    let listener = TcpListener::bind(&local_address)
        .await
        .expect("Failed to bind local address");
    println!("Listening on {}", local_address);

    // Create new graph instance
    let mut graph = Graph::new();

    match machine_id {
        // the order of communication is crucial for initial setup
        1 => {
            // 1 listens first, and needs to be launched first
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
            data_receiving_streams.insert(2, incoming_stream);
            graph
                .sending_streams
                .write()
                .await
                .insert(2, Mutex::new(outgoing_stream));
            graph
                .rpc_sending_streams
                .write()
                .await
                .insert(2, Mutex::new(rpc_sending_stream));
        }
        2 => {
            // 2 initiates outgoing connections first, needs to be launched second
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
            data_receiving_streams.insert(1, incoming_stream);
            graph
                .sending_streams
                .write()
                .await
                .insert(1, Mutex::new(outgoing_stream));
            graph
                .rpc_sending_streams
                .write()
                .await
                .insert(1, Mutex::new(rpc_sending_stream));
        }
        _ => unimplemented!(),
    }

    println!("Simulation Set Up Complete: Communication channels");

    // use graph builder to build the graph based on machine_id
    build_graph_integer_data(&mut graph, machine_id);

    let graph = Arc::new(graph);
    for (id, stream) in rpc_receiving_streams.into_iter() {
        let graph = graph.clone();
        tokio::spawn(async move {
            // handle_rpc_receiving_stream(&id, stream, &graph, GraphSum).await;
            handle_rpc_receiving_stream(&id, stream, graph, NaiveMaxAdjacentSum).await;
        });
    }

    for (_, mut data_receiving_stream) in data_receiving_streams.into_iter() {
        let graph = graph.clone();
        let dummy_session_control_data_len = bincode::serialize(&SessionControl {
            session_id: Uuid::default(),
            communication_type: DataType::Result,
            data_len: 0,
        })
        .unwrap()
        .len();

        tokio::spawn(async move {
            let mut session_control_bytes = vec![0u8; dummy_session_control_data_len];
            while let Ok(_) = data_receiving_stream
                .read_exact(&mut session_control_bytes)
                .await
            {
                println!("session_control data received: {:?}", session_control_bytes);
                let session_control =
                    bincode::deserialize::<SessionControl>(&session_control_bytes).unwrap();
                match &session_control.communication_type {
                    DataType::AuxInfo => {
                        unimplemented!()
                    }
                    DataType::Result => {
                        let res_channels = graph.result_multiplexing_channels.read().await;
                        let res_channel = res_channels
                            .get(&session_control.session_id)
                            .expect("session id not exist in aux_info channel")
                            .lock()
                            .await;

                        let mut res_bytes = vec![0u8; session_control.data_len];
                        data_receiving_stream
                            .read_exact(&mut res_bytes)
                            .await
                            .unwrap();
                        println!("res_bytes received: {:?}\n", res_bytes);

                        let res = bincode::deserialize::<isize>(&res_bytes).unwrap(); // Note: Make this generic soon
                        res_channel.send(res).await.unwrap();
                    }
                }
            }
        });
    }

    if machine_id == 1 {
        // Apply function and print result
        let root = graph.get(&0);
        let distance = 2;
        // let result = root.apply_function(&GraphSum, &graph, None).await;
        let result = root
            .apply_function(
                &NaiveMaxAdjacentSum,
                &graph,
                Some(NMASInfo {
                    source: None,
                    distance,
                    started: HashSet::new(),
                }),
            )
            .await;
        // println!("The graph sum is: {result}");
        println!("The Max Adjacent Sum for {distance} is: {result}");
    }

    loop {}
}

async fn handle_rpc_receiving_stream<
    V: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
    U: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
    T: UserDefinedFunction<V, U> + Send + Sync + 'static + Clone,
>(
    id: &MachineID,
    mut stream: TcpStream,
    graph: Arc<Graph<V, U>>,
    _type: T,
) {
    // receive fixed size of bytes for RPC

    let dummy_rpc = RPC::Execute(Uuid::default(), 0, 0);
    let dummy_rpc_len = bincode::serialize(&dummy_rpc).unwrap().len();

    let mut cmd = vec![0u8; dummy_rpc_len];
    println!("length of rpc: {:?}", dummy_rpc_len);

    while let Ok(_) = stream.read_exact(&mut cmd).await {
        println!("rpc received: {:?}", cmd);
        match bincode::deserialize::<RPC>(&cmd).expect("Incorrect RPC format") {
            RPC::Execute(uuid, v_id, aux_info_len) => {
                // TODO: Add Comments

                let mut aux_info = vec![0u8; aux_info_len];
                stream.read_exact(&mut aux_info).await.unwrap();
                println!("aux_info_bytes received: {:?}\n", aux_info);

                let aux_info =
                    bincode::deserialize::<U>(&aux_info).expect("Incorrect Auxiliary Info Format");

                // drop(receiving_stream);
                // drop(receiving_streams);

                let graph_clone = graph.clone();
                let _type_clone = _type.clone();
                let id_clone = id.clone();

                tokio::spawn(async move {
                    println!("in remote remote");
                    let res = graph_clone
                        .get(&v_id)
                        .apply_function(&_type_clone, &graph_clone, aux_info)
                        .await;
                    println!("remote remote done");

                    println!("trying to send back result");
                    // get sending_stream as mut
                    let sending_streams = graph_clone.sending_streams.read().await;
                    let mut sending_stream = sending_streams.get(&id_clone).unwrap().lock().await;
                    println!("gotten lock to send back result");

                    let res_bytes = bincode::serialize::<V>(&res).unwrap();

                    let session_control_result = SessionControl {
                        session_id: uuid,
                        communication_type: DataType::Result,
                        data_len: res_bytes.len(),
                    };

                    let session_control_res_bytes =
                        bincode::serialize(&session_control_result).unwrap();

                    println!(
                        "session_control_res_bytes sent: {:?}",
                        session_control_res_bytes
                    );
                    println!("res_bytes sent: {:?}\n", res_bytes);

                    sending_stream
                        .write_all(&[session_control_res_bytes, res_bytes].concat())
                        .await
                        .unwrap();
                    println!("sent back result successfully");
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
