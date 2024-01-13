use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};
use std::ops::AddAssign;

/* Type Aliases */
type VertexID = u32;
type MachineID = u32;

pub struct Data<T: DeserializeOwned>(pub T);

/* struct definitions */

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
    pub fn apply_function<F: UserDefinedFunction<T>>(&self, udf: &F, graph: &Graph<T>) -> T {
        match &self.v_type {
            VertexType::Local(_) | VertexType::Borrowed(_) => udf.execute(&self, graph),
            VertexType::Remote(remote_vertex) => {
                // Delegate to the remote machine: rpc here
                remote_vertex.remote_execute(self.id)
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
    id: VertexID,
    location: MachineID,
}
impl RemoteVertex {
    /*
       Constructor
    */
    pub fn new(id: VertexID, location: MachineID) -> Self {
        Self { id, location }
    }

    /*
       RPC
    */
    fn remote_execute<T>(&self, vertex_id: VertexID) -> T
    where
        T: DeserializeOwned,
    {
        // Implement RPC to send the function and necessary data to the remote machine.
        // The remote machine executes the function and returns the result.

        // should receive bytes
        // let result : vec<u8> = todo();

        let result_bytes: Vec<u8> = 100isize.to_ne_bytes().to_vec();
        if result_bytes.len() != std::mem::size_of::<T>() {
            panic!()
        }

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
}

impl<T: DeserializeOwned> Graph<T> {
    /*
       Constructor
    */
    pub fn new() -> Self {
        Graph {
            vertex_map: HashMap::new(),
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
                    v_type: VertexType::Remote(RemoteVertex::new(id, location)),
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

/*
   Trait of user-defined function requirements
*/
pub trait UserDefinedFunction<T: DeserializeOwned> {
    fn execute(&self, vertex: &Vertex<T>, graph: &Graph<T>) -> T;
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
impl UserDefinedFunction<isize> for GraphSum {
    fn execute(&self, vertex: &Vertex<isize>, graph: &Graph<isize>) -> isize {
        let mut count = Data(0);
        count += vertex.get_val().as_ref().unwrap().0;

        for sub_graph_root_id in vertex.children().iter() {
            count += graph
                .vertex_map
                .get(sub_graph_root_id)
                .expect("node not found")
                .apply_function(self, graph);
        }
        count.0
    }
}

// custom graph builder for testing
fn build_graph(graph: &mut Graph<isize>) {
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
    graph.add_new_vertex(8, &[4], &[], None, VertexKind::Remote, Some(1));
    graph.add_new_vertex(9, &[4], &[], None, VertexKind::Remote, Some(2));
    // Add more vertices as required
}

fn main() {
    let mut graph = Graph::new();
    build_graph(&mut graph);

    // Apply function and print result
    let result = graph.get(&0).unwrap().apply_function(&GraphSum, &graph);

    println!("The graph sum is: {result}");
}
