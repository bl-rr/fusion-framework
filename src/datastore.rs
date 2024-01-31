/* datastore.rs

   DataStore stores the mapping between VertexID and Vertex's
   This file contains all the mapping-related functions, an layer on top of the vertices

   Author: Binghong(Leo) Li
   Creation Date: 1/30/2024
*/

use crate::vertex::{
    Data, LocalVertex, MachineID, RemoteVertex, Vertex, VertexID, VertexKind, VertexType,
};
use crate::worker::Worker;

use hashbrown::HashMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

#[derive(Default)]
pub struct DataStore<T: Serialize + DeserializeOwned, V>(HashMap<VertexID, Vertex<T, V>>); // vertex_id -> vertex mapping

impl<T: Serialize + DeserializeOwned, V> DataStore<T, V> {
    /*
       Adding an existing Vertex
    */
    pub fn add_vertex(&mut self, v_id: VertexID, vertex: Vertex<T, V>) {
        self.0.insert(v_id, vertex);
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
        worker: Arc<Worker<T, V>>,
    ) {
        let vertex = match vertex_kind {
            VertexKind::Local => Vertex {
                id,
                v_type: VertexType::Local(LocalVertex::create_vertex(
                    incoming,
                    outgoing,
                    data.expect("Local vertex must have data."),
                    worker,
                )),
            },
            VertexKind::Remote => {
                let location = location.expect("Remote vertex must have a location.");
                Vertex {
                    id,
                    v_type: VertexType::Remote(RemoteVertex::new(location, worker)),
                }
            }
            VertexKind::Borrowed => Vertex {
                id,
                v_type: VertexType::Borrowed(LocalVertex::create_vertex(
                    incoming,
                    outgoing,
                    data.expect("Borrowed vertex must have data."),
                    worker,
                )),
            },
        };

        self.add_vertex(id, vertex);
    }

    // Getter, assumes no error
    pub fn get_vertex_by_id(&self, v_id: &VertexID) -> &Vertex<T, V> {
        self.0.get(v_id).expect("node not found")
    }
}

// custom graph builder for testing based on machine_id (the 1,2 scenario), for now
pub fn build_graph_integer_data(
    data_store: &mut DataStore<isize, isize>,
    machine_id: MachineID,
    worker: Arc<Worker<isize, isize>>,
) {
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
            data_store.add_new_vertex(
                0,
                &[],
                &[1, 2],
                Some(Data(1)),
                VertexKind::Local,
                None,
                worker.clone(),
            );

            // First level children
            data_store.add_new_vertex(
                1,
                &[0],
                &[3, 4],
                Some(Data(2)),
                VertexKind::Local,
                None,
                worker.clone(),
            );
            data_store.add_new_vertex(
                2,
                &[0],
                &[5, 6],
                Some(Data(3)),
                VertexKind::Local,
                None,
                worker.clone(),
            );

            // Second level children
            data_store.add_new_vertex(
                3,
                &[1],
                &[],
                Some(Data(4)),
                VertexKind::Local,
                None,
                worker.clone(),
            );
            data_store.add_new_vertex(
                4,
                &[1],
                &[7, 8, 9],
                Some(Data(5)),
                VertexKind::Local,
                None,
                worker.clone(),
            );
            data_store.add_new_vertex(
                5,
                &[2],
                &[],
                Some(Data(6)),
                VertexKind::Local,
                None,
                worker.clone(),
            );
            data_store.add_new_vertex(
                6,
                &[2],
                &[],
                Some(Data(7)),
                VertexKind::Local,
                None,
                worker.clone(),
            );

            // Third level child
            data_store.add_new_vertex(
                7,
                &[4],
                &[],
                Some(Data(8)),
                VertexKind::Local,
                None,
                worker.clone(),
            );
            data_store.add_new_vertex(
                8,
                &[4],
                &[],
                None,
                VertexKind::Remote,
                Some(2),
                worker.clone(),
            );
            data_store.add_new_vertex(
                9,
                &[4],
                &[],
                None,
                VertexKind::Remote,
                Some(2),
                worker.clone(),
            );
        }
        2 => {
            // Parent of the roots
            data_store.add_new_vertex(
                4,
                &[],
                &[8, 9],
                None,
                VertexKind::Remote,
                Some(1),
                worker.clone(),
            );

            // Root vertex
            data_store.add_new_vertex(
                8,
                &[4],
                &[10, 11],
                Some(Data(100)),
                VertexKind::Local,
                None,
                worker.clone(),
            );
            data_store.add_new_vertex(
                9,
                &[4],
                &[12, 13],
                Some(Data(200)),
                VertexKind::Local,
                None,
                worker.clone(),
            );

            // First level children
            data_store.add_new_vertex(
                10,
                &[8],
                &[],
                Some(Data(300)),
                VertexKind::Local,
                None,
                worker.clone(),
            );
            data_store.add_new_vertex(
                11,
                &[8],
                &[],
                Some(Data(400)),
                VertexKind::Local,
                None,
                worker.clone(),
            );
            data_store.add_new_vertex(
                12,
                &[9],
                &[],
                Some(Data(500)),
                VertexKind::Local,
                None,
                worker.clone(),
            );
            data_store.add_new_vertex(
                13,
                &[9],
                &[],
                Some(Data(600)),
                VertexKind::Local,
                None,
                worker.clone(),
            );
        }
        _ => {
            unimplemented!()
        }
    }
}
