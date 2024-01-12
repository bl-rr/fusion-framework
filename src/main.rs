#![feature(generic_const_exprs)]
use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio;
use tokio::sync::Mutex;

type VertexID = u32;
type MachineID = u32;
type Data = isize;

/*
    Graph Class that stores the (vertex_id -> vertex) mapping acting as pointers to vertices
*/
pub struct Graph {
    vertex_map: HashMap<VertexID, Vertex>,
}

// Enum to distinguish between different vertex kinds
pub enum VertexKind {
    Local,
    Remote,
    Borrowed,
}
impl Graph {
    pub fn new() -> Self {
        Graph {
            vertex_map: HashMap::new(),
        }
    }
    pub fn add_vertex(&mut self, v_id: VertexID, vertex: Vertex) {
        self.vertex_map.insert(v_id, vertex);
    }
    pub fn add_new_vertex(
        &mut self,
        id: VertexID,
        incoming: &[VertexID],
        outgoing: &[VertexID],
        data: Option<Data>,
        vertex_kind: VertexKind,
        location: Option<MachineID>,
    ) {
        let vertex = match vertex_kind {
            VertexKind::Local => Vertex {
                id,
                v_type: VertexType::Local(LocalVertex::create_vertex(
                    incoming,
                    outgoing,
                    data.unwrap_or_default(),
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
                    data.unwrap_or_default(),
                )),
            },
        };

        self.add_vertex(id, vertex);
    }
    pub fn get(&self, v_id: &VertexID) -> Option<&Vertex> {
        self.vertex_map.get(v_id)
    }
}

/* VertexType
   A vertex is either
        1)  local:      local data
        2)  remote:     remote reference of vertex living on another machine/core/node
        3)  borrowed:   brought to local, original copy resides in remote
*/
pub enum VertexType {
    Local(LocalVertex),
    Remote(RemoteVertex),
    Borrowed(LocalVertex),
}

/*
   Vertex
*/
pub struct Vertex {
    id: VertexID,
    v_type: VertexType,
}
impl Vertex {
    /* User-Defined_Function Invoker */
    pub fn apply_function<T: DeserializeOwned, F: UserDefinedFunction<T>>(
        &self,
        udf: &F,
        graph: &Graph,
    ) -> T {
        match &self.v_type {
            VertexType::Local(_) | VertexType::Borrowed(_) => udf.execute(&self, graph),
            VertexType::Remote(remote_vertex) => {
                // Delegate to the remote machine: rpc here
                remote_vertex.remote_execute(self.id)
            }
        }
    }

    // Vertex Interfaces
    pub fn children(&self) -> &HashSet<VertexID> {
        match &self.v_type {
            VertexType::Local(local_v) | VertexType::Borrowed(local_v) => local_v.children(),
            VertexType::Remote(_) => {
                // this should never be reached
                unimplemented!()
            }
        }
    }
    pub fn get_val(&self) -> &Option<Data> {
        match &self.v_type {
            VertexType::Local(local_v) | VertexType::Borrowed(local_v) => local_v.get_data(),
            VertexType::Remote(_) => {
                // this should never be reached
                unimplemented!()
            }
        }
    }
}

/*
   Vertex that resides locally, or borrowed to be temporarily locally
*/
pub struct LocalVertex {
    incoming_edges: HashSet<VertexID>, // for simulating trees, or DAGs
    outgoing_edges: HashSet<VertexID>, // for simulating trees, or DAGs
    edges: HashSet<VertexID>,          // for simulating general graphs
    data: Option<Data>,
    borrowed: bool, // for when other nodes are conducting operation on this
}
impl LocalVertex {
    pub fn new(
        incoming: HashSet<VertexID>,
        outgoing: HashSet<VertexID>,
        edges: HashSet<VertexID>,
        data: Option<Data>,
    ) -> Self {
        LocalVertex {
            incoming_edges: incoming,
            outgoing_edges: outgoing,
            edges,
            data,
            borrowed: false,
        }
    }
    fn create_vertex(incoming: &[VertexID], outgoing: &[VertexID], data: Data) -> Self {
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
    pub fn children(&self) -> &HashSet<VertexID> {
        &self.outgoing_edges
    }
    pub fn parents(&self) -> &HashSet<VertexID> {
        &self.incoming_edges
    }
    pub fn edges(&self) -> &HashSet<VertexID> {
        &self.edges
    }
    pub fn get_data(&self) -> &Option<Data> {
        &self.data
    }
    pub fn get_data_mut(&mut self) -> &mut Option<Data> {
        &mut self.data
    }
    pub fn set_data(&mut self, data: Data) -> Option<Data> {
        if self.borrowed {
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
    pub fn new(location: MachineID) -> Self {
        Self { location }
    }
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
   Trait of user-defined function requirements
*/
pub trait UserDefinedFunction<T> {
    fn execute(&self, vertex: &Vertex, graph: &Graph) -> T;
}

/* *********** Starting of User's Playground *********** */

struct GraphSum;
impl UserDefinedFunction<isize> for GraphSum {
    fn execute(&self, vertex: &Vertex, graph: &Graph) -> isize {
        let mut count = vertex.get_val().unwrap();
        for sub_graph_root_id in vertex.children().iter() {
            count += graph
                .vertex_map
                .get(sub_graph_root_id)
                .expect("node not found")
                .apply_function(self, graph);
        }
        count
    }
}

fn build_graph(graph: &mut Graph) {
    // Root vertex
    graph.add_new_vertex(0, &[], &[1, 2], Some(1), VertexKind::Local, None);

    // First level children
    graph.add_new_vertex(1, &[0], &[3, 4], Some(2), VertexKind::Local, None);
    graph.add_new_vertex(2, &[0], &[5, 6], Some(3), VertexKind::Local, None);

    // Second level children
    graph.add_new_vertex(3, &[1], &[], Some(4), VertexKind::Local, None);
    graph.add_new_vertex(4, &[1], &[7, 8, 9], Some(5), VertexKind::Local, None);
    graph.add_new_vertex(5, &[2], &[], Some(6), VertexKind::Local, None);
    graph.add_new_vertex(6, &[2], &[], Some(7), VertexKind::Local, None);

    // Third level child
    graph.add_new_vertex(7, &[4], &[], Some(8), VertexKind::Local, None);
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

    // Now, the graph has a complex tree structure.
    // You can further manipulate or query this graph as needed.
}
