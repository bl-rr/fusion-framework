#![feature(generic_const_exprs)]
use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio;
use tokio::sync::Mutex;

// Type aliases for clarity
type DataID = u32;

// type Result = Vec<u8>;

type VertexID = u32;
type MachineID = u32;
type Data = Vec<u8>;
pub enum VertexType {
    Local(LocalVertex),
    Remote(RemoteVertex),
    Borrowed(LocalVertex),
}

pub struct Vertex {
    id: VertexID,
    v_type: VertexType,
}

impl Vertex {
    pub fn apply_function<T: DeserializeOwned, F: UserDefinedFunction<T>>(
        &self,
        udf: &F,
        graph: &Graph,
    ) -> T {
        match &self.v_type {
            VertexType::Local(_) | VertexType::Borrowed(_) => udf.execute(&self.id, graph),
            VertexType::Remote(remote_vertex) => {
                // Delegate to the remote machine
                // rpc here
                remote_vertex.remote_execute(self.id)
            }
        }
    }

    pub fn children(&self) -> &HashSet<VertexID> {
        match &self.v_type {
            VertexType::Local(local_v) | VertexType::Borrowed(local_v) => local_v.children(),
            VertexType::Remote(remote_v) => {
                unimplemented!()
            }
        }
    }
}

pub trait UserDefinedFunction<T> {
    fn execute(&self, vertex: &VertexID, graph: &Graph) -> T;
}

struct GraphSum;
impl UserDefinedFunction<isize> for GraphSum {
    fn execute(&self, vertex_id: &VertexID, graph: &Graph) -> isize {
        // Implement the logic for graph_sum here.
        // This function can be recursive and will be applied to each vertex accordingly.

        let mut count = 0;
        for sub_graph_root_id in graph
            .get(&vertex_id)
            .expect("node not found")
            .children()
            .iter()
        {
            count += graph
                .vertex_map
                .get(sub_graph_root_id)
                .expect("node not found")
                .apply_function(self, graph);
        }
        count
    }
}

pub struct LocalVertex {
    id: VertexID,
    incoming_edges: HashSet<VertexID>, // for simulating trees, or DAGs
    outgoing_edges: HashSet<VertexID>, // for simulating trees, or DAGs
    edges: HashSet<VertexID>,          // for simulating general graphs
    data: Option<Data>,
    borrowed: bool, // for when other nodes are conducting operation on this
}

impl LocalVertex {
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

pub struct RemoteVertex {
    id: VertexID,
    location: MachineID,
}

impl RemoteVertex {
    fn remote_execute<T>(&self, vertex_id: VertexID) -> T
    where
        T: DeserializeOwned,
    {
        // Implement RPC to send the function and necessary data to the remote machine.
        // The remote machine executes the function and returns the result.

        // should receive bytes
        // let result : vec<u8> = todo();
        let result_bytes: Vec<u8> = [0u8; 8].to_vec();
        if result_bytes.len() != std::mem::size_of::<T>() {
            panic!()
        }

        bincode::deserialize(&result_bytes).unwrap()
    }
}
pub struct Graph {
    vertex_map: HashMap<VertexID, Vertex>,
}

impl Graph {
    pub fn get(&self, v_id: &VertexID) -> Option<&Vertex> {
        self.vertex_map.get(v_id)
    }
}
fn main() {}
