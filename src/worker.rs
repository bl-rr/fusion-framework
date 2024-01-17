/* worker
   Contains all the graph related structs and functions, an layer on top of the vertices

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2023
*/

use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::vertex::*;

/*
    Worker Struct that stores the (vertex_id -> vertex) mapping, acting as pointers to vertices
        as well as the communication channels

    TODO: Add weights to edges
*/
pub struct Worker<T: DeserializeOwned + Serialize> {
    pub graph: HashMap<VertexID, Vertex<T>>, // vertex_id -> vertex mapping
    pub sending_streams: RwLock<HashMap<MachineID, Mutex<TcpStream>>>,
    pub rpc_sending_streams: RwLock<HashMap<MachineID, Mutex<TcpStream>>>,
    pub result_multiplexing_channels: RwLock<HashMap<Uuid, Mutex<Sender<T>>>>,
}

impl<T: DeserializeOwned + Serialize> Worker<T> {
    /*
       Constructor
    */
    pub fn new() -> Self {
        Worker {
            graph: HashMap::new(),
            sending_streams: RwLock::new(HashMap::new()),
            rpc_sending_streams: RwLock::new(HashMap::new()),
            result_multiplexing_channels: RwLock::new(HashMap::new()),
        }
    }

    /*
       Adding an existing Vertex
    */
    pub fn add_vertex(&mut self, v_id: VertexID, vertex: Vertex<T>) {
        self.graph.insert(v_id, vertex);
    }

    /*
       Adding a vertex from scratch
    */
    pub fn add_new_vertex(
        &mut self,
        id: VertexID,
        incoming: &[VertexID],
        outgoing: &[VertexID],
        data: Option<Data<T>>,       // only exists for local nodes
        vertex_kind: VertexKind,     // determining the type of node (remote | local)
        location: Option<MachineID>, // only exists for remote nodes
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

    // Getter, assumes no error
    pub fn get_vertex_by_id(&self, v_id: &VertexID) -> &Vertex<T> {
        self.graph.get(v_id).expect("node not found")
    }
}

// custom graph builder for testing based on machine_id (the 1,2 scenario), for now
pub fn build_graph_integer_data(worker: &mut Worker<isize>, machine_id: MachineID) {
    // Note: this is specific testing function

    //             Node 1:
    //                  0
    //                 / \
    //                /   \
    //               /     \
    //              1       2
    //             / \     / \
    //            3   4   5   6
    //               /|\
    //              / | \
    //             7  8  9
    //              (R2) (R2)
    //
    //             Node 2:
    //                  4 (R1)
    //                 / \
    //                /   \
    //               8     9
    //              / \   / \
    //             10 11 12 13

    match machine_id {
        1 => {
            // Root vertex
            worker.add_new_vertex(0, &[], &[1, 2], Some(Data(1)), VertexKind::Local, None);

            // First level children
            worker.add_new_vertex(1, &[0], &[3, 4], Some(Data(2)), VertexKind::Local, None);
            worker.add_new_vertex(2, &[0], &[5, 6], Some(Data(3)), VertexKind::Local, None);

            // Second level children
            worker.add_new_vertex(3, &[1], &[], Some(Data(4)), VertexKind::Local, None);
            worker.add_new_vertex(4, &[1], &[7, 8, 9], Some(Data(5)), VertexKind::Local, None);
            worker.add_new_vertex(5, &[2], &[], Some(Data(6)), VertexKind::Local, None);
            worker.add_new_vertex(6, &[2], &[], Some(Data(7)), VertexKind::Local, None);

            // Third level child
            worker.add_new_vertex(7, &[4], &[], Some(Data(8)), VertexKind::Local, None);
            worker.add_new_vertex(8, &[4], &[], None, VertexKind::Remote, Some(2));
            worker.add_new_vertex(9, &[4], &[], None, VertexKind::Remote, Some(2));
        }
        2 => {
            // Parent of the roots
            worker.add_new_vertex(4, &[], &[8, 9], None, VertexKind::Remote, Some(1));

            // Root vertex
            worker.add_new_vertex(8, &[4], &[10, 11], Some(Data(100)), VertexKind::Local, None);
            worker.add_new_vertex(9, &[4], &[12, 13], Some(Data(200)), VertexKind::Local, None);

            // First level children
            worker.add_new_vertex(10, &[8], &[], Some(Data(300)), VertexKind::Local, None);
            worker.add_new_vertex(11, &[8], &[], Some(Data(400)), VertexKind::Local, None);
            worker.add_new_vertex(12, &[9], &[], Some(Data(500)), VertexKind::Local, None);
            worker.add_new_vertex(13, &[9], &[], Some(Data(600)), VertexKind::Local, None);
        }
        _ => {
            unimplemented!()
        }
    }
}
