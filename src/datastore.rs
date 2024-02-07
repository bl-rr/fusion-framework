/* datastore.rs

   DataStore stores the mapping between VertexID and Vertex's
   This file contains all the mapping-related functions, a layer on top of the vertices

   Author: Binghong(Leo) Li
   Creation Date: 1/30/2024
*/

use core::fmt::{self, Debug};

use crate::vertex::{
    Data, LocalVertex, MachineID, RemoteVertex, Vertex, VertexID, VertexKind, VertexType,
};
use crate::worker::Worker;

use hashbrown::HashMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct DataStore<T: Serialize + DeserializeOwned + Debug + Default, V: Debug> {
    pub map: HashMap<VertexID, Vertex<T, V>>,
    pub new_nodes: Mutex<Vec<Vertex<T, V>>>,
    pub nodes_to_delete: Mutex<Vec<Vertex<T, V>>>,
    pub next_id: VertexID,
} // vertex_id -> vertex mapping

impl<T, V> Debug for DataStore<T, V>
where
    T: Serialize + DeserializeOwned + Debug + Default,
    V: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "DataStore {{")?;
        for (_, vertex) in &self.map {
            writeln!(f, "\t{:?}", vertex)?;
        }
        write!(f, "}}")
    }
}

impl<T: Serialize + DeserializeOwned + Debug + Default, V: Debug> DataStore<T, V> {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            new_nodes: Default::default(),
            nodes_to_delete: Default::default(),
            next_id: 1,
        }
    }
    /*
       Adding an existing Vertex
    */
    pub fn add_vertex(&mut self, v_id: VertexID, vertex: Vertex<T, V>) {
        self.next_id += 1;
        self.map.insert(v_id, vertex);
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
                )),
            },
        };

        self.add_vertex(id, vertex);
    }

    // Getter, assumes no error
    pub fn get_vertex_by_id(&self, v_id: &VertexID) -> &Vertex<T, V> {
        self.map.get(v_id).expect("node not found")
    }

    pub async fn update(&mut self) {
        // TODO: Implement Actual update logic
        let mut new_nodes = self.new_nodes.lock().await;

        for new_node in new_nodes.drain(..) {
            // todo
            self.map.insert(self.next_id, new_node);
            self.next_id += 1;
        }
    }
}

// custom graph builder for testing based on machine_id (the 1,2 scenario), for now
pub fn build_graph_integer_data<V: Debug>(
    data_store: &mut DataStore<isize, V>,
    machine_id: MachineID,
    worker: Arc<Worker<isize, V>>,
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
                // Some(Data(-10000)),
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
                // Some(Data(-1000)),
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

            // adding pure remote nodes, don't need to know where they are
            data_store.add_new_vertex(
                10,
                &[],
                &[],
                None,
                VertexKind::Remote,
                Some(2),
                worker.clone(),
            );
            data_store.add_new_vertex(
                11,
                &[],
                &[],
                None,
                VertexKind::Remote,
                Some(2),
                worker.clone(),
            );
            data_store.add_new_vertex(
                12,
                &[],
                &[],
                None,
                VertexKind::Remote,
                Some(2),
                worker.clone(),
            );
            data_store.add_new_vertex(
                13,
                &[],
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
                // Some(Data(1000)),
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

            // adding pure remote nodes, don't need to know where they are
            data_store.add_new_vertex(
                0,
                &[],
                &[],
                None,
                VertexKind::Remote,
                Some(1),
                worker.clone(),
            );
            data_store.add_new_vertex(
                1,
                &[],
                &[],
                None,
                VertexKind::Remote,
                Some(1),
                worker.clone(),
            );
            data_store.add_new_vertex(
                2,
                &[],
                &[],
                None,
                VertexKind::Remote,
                Some(1),
                worker.clone(),
            );
            data_store.add_new_vertex(
                3,
                &[],
                &[],
                None,
                VertexKind::Remote,
                Some(1),
                worker.clone(),
            );
            data_store.add_new_vertex(
                4,
                &[],
                &[],
                None,
                VertexKind::Remote,
                Some(1),
                worker.clone(),
            );
            data_store.add_new_vertex(
                5,
                &[],
                &[],
                None,
                VertexKind::Remote,
                Some(1),
                worker.clone(),
            );
            data_store.add_new_vertex(
                6,
                &[],
                &[],
                None,
                VertexKind::Remote,
                Some(1),
                worker.clone(),
            );
            data_store.add_new_vertex(
                7,
                &[],
                &[],
                None,
                VertexKind::Remote,
                Some(1),
                worker.clone(),
            );
        }
        _ => {
            unimplemented!()
        }
    }
}
