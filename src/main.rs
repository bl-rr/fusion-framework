use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ops::AddAssign;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};

/* Type Aliases */
type VertexID = u32;
type MachineID = u32;
type DummyID = u32;

/* struct definitions */

/*
   Data Wrapper
*/
pub struct Data<T: DeserializeOwned>(pub T);

/* VertexType
   A vertex is either
        1)  local:      local data
        2)  remote:     remote reference of vertex that lives on another machine/core/node
        3)  borrowed:   brought to local, original copy resides in remote (protected when leased?)
*/
pub enum VertexType<T: DeserializeOwned> {
    Local(LocalVertex<T>),
    Remote(RemoteVertex),
    Borrowed(LocalVertex<T>),
}

/*
   Vertex
*/
pub struct Vertex<T: DeserializeOwned> {
    id: VertexID,
    v_type: VertexType<T>,
}
impl<T: DeserializeOwned> Vertex<T> {
    /*
        User-Defined_Function Invoker

            T: the output of the UDF, needs to be deserializable for rpc
            F: UDF that defines the execute function
    */
    pub async fn apply_function<F: UserDefinedFunction<T>>(&self, udf: &F, graph: &Graph<T>) -> T {
        match &self.v_type {
            VertexType::Local(_) | VertexType::Borrowed(_) => udf.execute(&self, graph).await,
            VertexType::Remote(remote_vertex) => {
                // Delegate to the remote machine: rpc here
                remote_vertex.remote_execute(self.id, graph).await
            }
        }
    }

    /* Vertex Interfaces
       To allow local_vertex type functions to be called by the outer vertex struct
       Note: this is doable because the functions should never be invoked by a remote_vertex, or there are bugs
    */
    pub fn children(&self) -> &HashSet<VertexID> {
        match &self.v_type {
            VertexType::Local(local_v) | VertexType::Borrowed(local_v) => local_v.children(),
            VertexType::Remote(_) => {
                // this should never be reached
                panic!("Remote Node should not invoke children() function")
            }
        }
    }
    pub fn get_val(&self) -> &Option<Data<T>> {
        match &self.v_type {
            VertexType::Local(local_v) | VertexType::Borrowed(local_v) => local_v.get_data(),
            VertexType::Remote(_) => {
                // this should never be reached
                panic!("Remote Node should not invoke get_val() function")
            }
        }
    }
}

/*
   Vertex that resides locally, or borrowed to be temporarily locally
*/
pub struct LocalVertex<T: DeserializeOwned> {
    incoming_edges: HashSet<VertexID>, // for simulating trees, or DAGs
    outgoing_edges: HashSet<VertexID>, // for simulating trees, or DAGs
    edges: HashSet<VertexID>,          // for simulating general graphs
    data: Option<Data<T>>, // Using option to return the previous value (for error checking, etc.)
    borrowed_in: bool,     // When a node is a borrowed node
    leased_out: bool,      // When the current node is lent out
}
impl<T: DeserializeOwned> LocalVertex<T> {
    /*
       Constructor
    */
    pub fn new(
        incoming: HashSet<VertexID>,
        outgoing: HashSet<VertexID>,
        edges: HashSet<VertexID>,
        data: Option<Data<T>>,
    ) -> Self {
        LocalVertex {
            incoming_edges: incoming,
            outgoing_edges: outgoing,
            edges,
            data,
            borrowed_in: false,
            leased_out: false,
        }
    }

    /*
       Builder/Creator method for easier construction in graph constructors
    */
    pub fn create_vertex(incoming: &[VertexID], outgoing: &[VertexID], data: Data<T>) -> Self {
        LocalVertex::new(
            incoming.iter().cloned().collect(),
            outgoing.iter().cloned().collect(),
            [incoming.to_vec(), outgoing.to_vec()]
                .concat()
                .iter()
                .cloned()
                .collect(),
            Some(data),
        )
    }

    // getters and setters
    pub fn children(&self) -> &HashSet<VertexID> {
        &self.outgoing_edges
    }
    pub fn parents(&self) -> &HashSet<VertexID> {
        &self.incoming_edges
    }
    pub fn edges(&self) -> &HashSet<VertexID> {
        &self.edges
    }
    pub fn get_data(&self) -> &Option<Data<T>> {
        &self.data
    }
    pub fn get_data_mut(&mut self) -> &mut Option<Data<T>> {
        &mut self.data
    }
    pub fn set_data(&mut self, data: Data<T>) -> Option<Data<T>> {
        if self.leased_out {
            None
        } else {
            self.data.replace(data)
        }
    }
}

/*
   Remote References to other vertices
*/
pub struct RemoteVertex {
    location: MachineID,
}
impl RemoteVertex {
    /*
       Constructor
    */
    pub fn new(location: MachineID) -> Self {
        Self { location }
    }

    /*
       RPC
    */
    async fn remote_execute<T>(&self, vertex_id: VertexID, graph: &Graph<T>) -> T
    where
        T: DeserializeOwned,
    {
        // The remote machine executes the function and returns the result.

        println!("starting remote Execute");

        let rpc_sending_streams = graph.rpc_sending_streams.read().await;
        let mut rpc_sending_stream = rpc_sending_streams
            .get(&self.location)
            .unwrap()
            .lock()
            .await;

        let command = bincode::serialize(&RPC::Execute(vertex_id, 0)).unwrap();
        // let command = bincode::serialize(&RPC::Relay(vertex_id, 0)).unwrap();
        // let _command = bincode::serialize(&RPC::Relay(vertex_id, 1)).unwrap();
        // println!("Sent Size Is for command: {}", command.len());
        // println!("Sent Size Is for _command: {}", _command.len());

        rpc_sending_stream.write_all(&command).await.unwrap();

        println!("sent rpc command to {}", self.location);
        drop(rpc_sending_stream);
        drop(rpc_sending_streams);

        let mut result_bytes = vec![0u8; std::mem::size_of::<T>()];

        let receiving_streams = graph.receiving_streams.read().await;
        let mut receiving_stream = receiving_streams.get(&self.location).unwrap().lock().await;

        receiving_stream
            .read_exact(&mut result_bytes)
            .await
            .unwrap();

        println!("received rpc response");
        bincode::deserialize(&result_bytes).unwrap()
    }
}

/*
   Enum to distinguish between different vertex kinds, for graph constructing
*/
pub enum VertexKind {
    Local,
    Remote,
    Borrowed,
}

/*
    Graph Class that stores the (vertex_id -> vertex) mapping, acting as pointers to vertices
*/
pub struct Graph<T: DeserializeOwned> {
    vertex_map: HashMap<VertexID, Vertex<T>>,
    sending_streams: RwLock<HashMap<MachineID, Mutex<TcpStream>>>,
    receiving_streams: RwLock<HashMap<MachineID, Mutex<TcpStream>>>,
    rpc_sending_streams: RwLock<HashMap<MachineID, Mutex<TcpStream>>>,
}

impl<T: DeserializeOwned> Graph<T> {
    /*
       Constructor
    */
    pub fn new() -> Self {
        Graph {
            vertex_map: HashMap::new(),
            sending_streams: RwLock::new(HashMap::new()),
            receiving_streams: RwLock::new(HashMap::new()),
            rpc_sending_streams: RwLock::new(HashMap::new()),
        }
    }

    /*
       Adding an existing Vertex
    */
    pub fn add_vertex(&mut self, v_id: VertexID, vertex: Vertex<T>) {
        self.vertex_map.insert(v_id, vertex);
    }

    /*
       Adding a vertex from scratch
    */
    pub fn add_new_vertex(
        &mut self,
        id: VertexID,
        incoming: &[VertexID],
        outgoing: &[VertexID],
        data: Option<Data<T>>,
        vertex_kind: VertexKind,
        location: Option<MachineID>,
    ) {
        let vertex = match vertex_kind {
            VertexKind::Local => Vertex {
                id,
                v_type: VertexType::Local(LocalVertex::create_vertex(
                    incoming,
                    outgoing,
                    data.expect("Local vertex must have data."),
                )),
            },
            VertexKind::Remote => {
                let location = location.expect("Remote vertex must have a location.");
                Vertex {
                    id,
                    v_type: VertexType::Remote(RemoteVertex::new(location)),
                }
            }
            VertexKind::Borrowed => Vertex {
                id,
                v_type: VertexType::Borrowed(LocalVertex::create_vertex(
                    incoming,
                    outgoing,
                    data.expect("Borrowed vertex must have data."),
                )),
            },
        };

        self.add_vertex(id, vertex);
    }

    // Getter
    pub fn get(&self, v_id: &VertexID) -> Option<&Vertex<T>> {
        self.vertex_map.get(v_id)
    }
}

#[derive(Serialize, Deserialize)]
pub enum RPC {
    Execute(VertexID, DummyID),
    Relay(VertexID, MachineID),
    RequestData(VertexID, DummyID),
    ExecuteWithData(VertexID, DummyID),
    Update(VertexID, DummyID),
}

/*
   Trait of user-defined function requirements
*/
#[async_trait]
pub trait UserDefinedFunction<T: DeserializeOwned> {
    async fn execute(&self, vertex: &Vertex<T>, graph: &Graph<T>) -> T;
}

/* *********** Starting of User's Playground *********** */

/*
   Data<isize> operations
*/
impl AddAssign<isize> for Data<isize> {
    fn add_assign(&mut self, other: isize) {
        self.0 += other;
    }
}

// UDF Struct
struct GraphSum;
#[async_trait]
impl UserDefinedFunction<isize> for GraphSum {
    async fn execute(&self, vertex: &Vertex<isize>, graph: &Graph<isize>) -> isize {
        let mut count = Data(0);
        count += vertex.get_val().as_ref().unwrap().0;

        for sub_graph_root_id in vertex.children().iter() {
            count += graph
                .get(sub_graph_root_id)
                .expect("node not found")
                .apply_function(self, graph)
                .await;
        }
        count.0
    }
}

// custom graph builder for testing
fn build_graph(graph: &mut Graph<isize>, machine_id: MachineID) {
    match machine_id {
        1 => {
            // Root vertex
            graph.add_new_vertex(0, &[], &[1, 2], Some(Data(1)), VertexKind::Local, None);

            // First level children
            graph.add_new_vertex(1, &[0], &[3, 4], Some(Data(2)), VertexKind::Local, None);
            graph.add_new_vertex(2, &[0], &[5, 6], Some(Data(3)), VertexKind::Local, None);

            // Second level children
            graph.add_new_vertex(3, &[1], &[], Some(Data(4)), VertexKind::Local, None);
            graph.add_new_vertex(4, &[1], &[7, 8, 9], Some(Data(5)), VertexKind::Local, None);
            graph.add_new_vertex(5, &[2], &[], Some(Data(6)), VertexKind::Local, None);
            graph.add_new_vertex(6, &[2], &[], Some(Data(7)), VertexKind::Local, None);

            // Third level child
            graph.add_new_vertex(7, &[4], &[], Some(Data(8)), VertexKind::Local, None);
            graph.add_new_vertex(8, &[4], &[], None, VertexKind::Remote, Some(2));
            graph.add_new_vertex(9, &[4], &[], None, VertexKind::Remote, Some(2));
        }
        2 => {
            // Root vertex
            graph.add_new_vertex(8, &[], &[10, 11], Some(Data(100)), VertexKind::Local, None);
            graph.add_new_vertex(9, &[], &[12, 13], Some(Data(200)), VertexKind::Local, None);

            // First level children
            graph.add_new_vertex(10, &[8], &[], Some(Data(300)), VertexKind::Local, None);
            graph.add_new_vertex(11, &[8], &[], Some(Data(400)), VertexKind::Local, None);
            graph.add_new_vertex(12, &[9], &[], Some(Data(500)), VertexKind::Local, None);
            graph.add_new_vertex(13, &[9], &[], Some(Data(600)), VertexKind::Local, None);
        }
        _ => {
            unimplemented!()
        }
    }
}

async fn handle_node(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer).await {
            Ok(size) if size == 0 => return, // Connection was closed
            Ok(size) => {
                stream
                    .write_all(&buffer[..size])
                    .await
                    .expect("Failed to write to stream");
            }
            Err(e) => {
                eprintln!("An error occurred: {}", e);
                return;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <machine_id>", args[0]);
        std::process::exit(1);
    }
    let machine_id: MachineID = args[1].parse().expect("Invalid machine ID");

    let base_port = 8080;
    let local_port = base_port + machine_id;
    let remote_port = base_port + if machine_id == 1 { 2 } else { 1 };
    let local_address = format!("127.0.0.1:{}", local_port);
    let remote_address = format!("127.0.0.1:{}", remote_port);

    let mut rpc_receiving_streams = HashMap::new();

    // Start listening on the local port
    let listener = TcpListener::bind(&local_address)
        .await
        .expect("Failed to bind local address");
    println!("Listening on {}", local_address);

    let mut graph = Graph::new();

    match machine_id {
        1 => {
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

            rpc_receiving_streams.insert(2, rpc_receiving_stream);

            let outgoing_stream = TcpStream::connect(&remote_address)
                .await
                .expect(&*format!("Failed to connect to {remote_address}"));
            let rpc_sending_stream = TcpStream::connect(&remote_address)
                .await
                .expect(&*format!("Failed to connect to {remote_address}"));

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
            let outgoing_stream = TcpStream::connect(&remote_address)
                .await
                .expect(&*format!("Failed to connect to {remote_address}"));

            let rpc_sending_stream = TcpStream::connect(&remote_address)
                .await
                .expect(&*format!("Failed to connect to {remote_address}"));

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

    println!("Simulation TCP Set Up Complete");

    build_graph(&mut graph, machine_id);

    let graph = Arc::new(graph);

    for (id, mut stream) in rpc_receiving_streams.into_iter() {
        let graph = graph.clone();
        tokio::spawn(async move {
            let mut cmd = vec![0u8; std::mem::size_of::<RPC>()];
            println!("Size is {}", std::mem::size_of::<RPC>());
            // let mut _cmd = vec![0u8; 1];
            while let Ok(_) = stream.read_exact(&mut cmd).await {
                println!("rpc receiving stream received command");
                match bincode::deserialize::<RPC>(&cmd).expect("Incorrect RPC format") {
                    RPC::Execute(v_id, _) => {
                        println!("received plain remote execute");
                        let res = graph
                            .get(&v_id)
                            .unwrap()
                            .apply_function(&GraphSum, &graph)
                            .await;
                        println!("remote execute finished");
                        let sending_streams = graph.sending_streams.read().await;
                        let mut sending_stream = sending_streams.get(&id).unwrap().lock().await;
                        sending_stream
                            .write_all(&bincode::serialize(&res).unwrap())
                            .await
                            .unwrap();
                        println!("send remote execute response");
                    }
                    RPC::Relay(_, _) => {
                        unimplemented!()
                    }
                    RPC::RequestData(_, _) => {
                        unimplemented!()
                    }
                    RPC::ExecuteWithData(_, _) => {
                        unimplemented!()
                    }
                    RPC::Update(_, _) => {
                        unimplemented!()
                    }
                }
            }
        });
    }

    if machine_id == 1 {
        // Apply function and print result
        let root = graph.get(&0).unwrap();
        let result = root.apply_function(&GraphSum, &graph).await;
        println!("The graph sum is: {result}");
    }

    loop {}
}
