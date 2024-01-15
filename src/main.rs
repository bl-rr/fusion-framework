/* main.rs
   Testing Ground as of now

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2023
*/

use fusion_framework::graph::{build_graph, Graph};
use fusion_framework::rpc::RPC;
use fusion_framework::udf::{Direction, GraphSum, NMASInfo, NaiveMaxAdjacentSum};
use fusion_framework::vertex::MachineID;

use fusion_framework::UserDefinedFunction;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

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
            graph
                .sending_streams
                .write()
                .await
                .insert(2, Mutex::new(outgoing_stream));
            graph
                .receiving_streams
                .write()
                .await
                .insert(2, Mutex::new(incoming_stream));
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
            graph
                .sending_streams
                .write()
                .await
                .insert(1, Mutex::new(outgoing_stream));
            graph
                .receiving_streams
                .write()
                .await
                .insert(1, Mutex::new(incoming_stream));
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
    build_graph(&mut graph, machine_id);

    let graph = Arc::new(graph);
    for (id, stream) in rpc_receiving_streams.into_iter() {
        let graph = graph.clone();
        tokio::spawn(async move {
            // handle_rpc_receiving_stream(&id, stream, &graph, GraphSum).await;
            handle_rpc_receiving_stream(&id, stream, &graph, NaiveMaxAdjacentSum).await;
        });
    }

    if machine_id == 1 {
        // Apply function and print result
        let root = graph.get(&0);
        let distance = 1;
        // let result = root.apply_function(&GraphSum, &graph, None).await;
        let result = root
            .apply_function(
                &NaiveMaxAdjacentSum,
                &graph,
                Some(NMASInfo {
                    direction: Direction::Start,
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
    V: Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
    T: UserDefinedFunction<V, U>,
>(
    id: &MachineID,
    mut stream: TcpStream,
    graph: &Arc<Graph<V>>,
    _type: T,
) {
    // receive fixed size of bytes for RPC
    let mut cmd = vec![0u8; std::mem::size_of::<RPC>()];

    while let Ok(_) = stream.read_exact(&mut cmd).await {
        match bincode::deserialize::<RPC>(&cmd).expect("Incorrect RPC format") {
            RPC::Execute(v_id) => {
                // TODO: Add Comments
                let mut size: [u8; 8] = [0u8; std::mem::size_of::<usize>()];
                let receiving_streams = graph.receiving_streams.read().await;
                let mut receiving_stream = receiving_streams.get(&id).unwrap().lock().await;
                receiving_stream.read_exact(&mut size).await.unwrap();

                let size: usize = usize::from_ne_bytes(size);
                let mut aux_info = vec![0u8; size];
                println!("aux_info len recv: {}", aux_info.len());

                receiving_stream.read_exact(&mut aux_info).await.unwrap();

                let aux_info =
                    bincode::deserialize::<U>(&aux_info).expect("Incorrect Auxiliary Info Format");

                drop(receiving_stream);
                drop(receiving_streams);

                let res = graph
                    .get(&v_id)
                    .apply_function(&_type, &graph, aux_info)
                    .await;

                // get sending_stream as mut
                let sending_streams = graph.sending_streams.read().await;
                let mut sending_stream = sending_streams.get(&id).unwrap().lock().await;

                sending_stream
                    .write_all(&bincode::serialize(&res).unwrap())
                    .await
                    .unwrap();
            }
            RPC::Relay(_) => {
                unimplemented!()
            }
            RPC::RequestData(_) => {
                unimplemented!()
            }
            RPC::ExecuteWithData(_) => {
                unimplemented!()
            }
            RPC::Update(_) => {
                unimplemented!()
            }
        }
    }
}
