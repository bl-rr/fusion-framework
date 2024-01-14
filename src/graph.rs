/* graph.rs
   Contains all the graph related structs and functions, an layer on top of the vertices

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2023
*/

use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};

use crate::vertex::*;

/*
    Graph Struct that stores the (vertex_id -> vertex) mapping, acting as pointers to vertices
*/
pub struct Graph<T: DeserializeOwned + Serialize> {
    pub vertex_map: HashMap<VertexID, Vertex<T>>,
    pub sending_streams: RwLock<HashMap<MachineID, Mutex<TcpStream>>>,
    pub receiving_streams: RwLock<HashMap<MachineID, Mutex<TcpStream>>>,
    pub rpc_sending_streams: RwLock<HashMap<MachineID, Mutex<TcpStream>>>,
}

impl<T: DeserializeOwned + Serialize> Graph<T> {
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

// custom graph builder for testing based on machine_id (the 1,2 scenario), for now
pub fn build_graph(graph: &mut Graph<isize>, machine_id: MachineID) {
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
