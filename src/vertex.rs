/* vertex.rs

   Contains all the vertex related structs and functions, an layer on top of Vanilla Data

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2024
*/
use core::fmt;

use crate::datastore::DataStore;
use crate::{rpc, worker::Worker, UserDefinedFunction};

use hashbrown::HashSet;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex, MutexGuard};
use uuid::Uuid;

/* *********** Type Aliases *********** */
pub type VertexID = u32;
pub type MachineID = u32;

/* *********** struct definitions *********** */

/*
   Data Wrapper
*/
#[derive(Serialize, Debug)]
pub struct Data<T: DeserializeOwned>(pub T);

/* VertexType
   A vertex is either
        1)  local:      local data
        2)  remote:     remote reference of vertex that lives on another machine/core/node
        3)  borrowed:   brought to local, original copy resides in remote (protected when leased?)
*/
#[derive(Debug)]
pub enum VertexType<T: DeserializeOwned + Serialize + fmt::Debug, V> {
    Local(LocalVertex<T, V>),
    Remote(RemoteVertex<T, V>),
    Borrowed(LocalVertex<T, V>),
    // Note: maybe a (Leased) variant for the future?
}

/*
   Vertex
*/
#[derive(Debug)]
pub struct Vertex<T: DeserializeOwned + Serialize + fmt::Debug, V> {
    pub id: VertexID,
    pub v_type: VertexType<T, V>,
}
impl<T: DeserializeOwned + Serialize + fmt::Debug, V> Vertex<T, V> {
    /*
        User-Defined_Function Invoker

            T: the output of the UDF, needs to be deserializable for rpc
            F: UDF that defines the execute function
    */
    pub async fn apply_function<
        F: UserDefinedFunction<T, U, V>,
        U: Serialize + DeserializeOwned,
    >(
        &self,
        udf: &F,
        data_store: &DataStore<T, V>,
        auxiliary_information: U,
    ) -> V {
        match &self.v_type {
            VertexType::Local(_) | VertexType::Borrowed(_) => {
                udf.execute(&self, data_store, auxiliary_information).await
            }
            VertexType::Remote(remote_vertex) => {
                // Delegate to the remote machine: rpc here
                remote_vertex
                    .remote_execute(self.id, auxiliary_information)
                    .await
            }
        }
    }

    /* Vertex Interfaces
       To allow local_vertex type functions to be called by the outer vertex struct
       Note: these are doable because the functions should never be invoked by a remote_vertex, or there are bugs
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
    pub fn parents(&self) -> &HashSet<VertexID> {
        match &self.v_type {
            VertexType::Local(local_v) | VertexType::Borrowed(local_v) => local_v.parents(),
            VertexType::Remote(_) => {
                // this should never be reached
                panic!("Remote Node should not invoke parents() function")
            }
        }
    }
    pub fn edges(&self) -> &HashSet<VertexID> {
        match &self.v_type {
            VertexType::Local(local_v) | VertexType::Borrowed(local_v) => local_v.edges(),
            VertexType::Remote(_) => {
                // this should never be reached
                panic!("Remote Node should not invoke edges() function")
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
    pub async fn update(&self, data: Data<T>) -> Option<Data<T>> {
        match &self.v_type {
            VertexType::Local(local_v) | VertexType::Borrowed(local_v) => {
                local_v.set_data(data, self.id).await
            }
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
pub struct LocalVertex<T: DeserializeOwned + Serialize + fmt::Debug, V> {
    incoming_edges: HashSet<VertexID>, // for simulating trees, or DAGs
    outgoing_edges: HashSet<VertexID>, // for simulating trees, or DAGs
    edges: HashSet<VertexID>,          // for simulating general graphs
    data: Option<Data<T>>, // Using option to return the previous value (for error checking, etc.)
    borrowed_in: bool,     // When a node is a borrowed node
    leased_out: bool,      // When the current node is lent out
    worker: Arc<Worker<T, V>>,
}

impl<T: DeserializeOwned + Serialize + fmt::Debug, V> fmt::Debug for LocalVertex<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalVertex")
            .field("incoming_edges", &self.incoming_edges)
            .field("outgoing_edges", &self.outgoing_edges)
            .field("edges", &self.edges)
            .field("data", &self.data)
            .finish()
    }
}

impl<T: DeserializeOwned + Serialize + fmt::Debug, V> LocalVertex<T, V> {
    /*
       Constructor
    */
    pub fn new(
        incoming: HashSet<VertexID>,
        outgoing: HashSet<VertexID>,
        edges: HashSet<VertexID>,
        data: Option<Data<T>>,
        worker: Arc<Worker<T, V>>,
    ) -> Self {
        LocalVertex {
            incoming_edges: incoming,
            outgoing_edges: outgoing,
            edges,
            data,
            borrowed_in: false,
            leased_out: false,
            worker,
        }
    }

    /*
       Builder/Creator method for easier construction in graph constructors
       or in general when creating individual vertices
    */
    pub fn create_vertex(
        incoming: &[VertexID],
        outgoing: &[VertexID],
        data: Data<T>,
        worker: Arc<Worker<T, V>>,
    ) -> Self {
        LocalVertex::new(
            incoming.iter().cloned().collect(),
            outgoing.iter().cloned().collect(),
            [incoming.to_vec(), outgoing.to_vec()]
                .concat()
                .iter()
                .cloned()
                .collect(),
            Some(data),
            worker,
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
    pub async fn set_data(&self, data: Data<T>, self_id: VertexID) -> Option<Data<T>> {
        if self.leased_out {
            None
        } else {
            let old_val;
            let mut vertices_being_written: MutexGuard<HashSet<VertexID>> =
                self.worker.vertices_being_written.lock().await;

            // Note: The CondVar is not "Cancellation Safe", yet CondVar would be the most appropriate construct here
            while vertices_being_written.contains(&self_id) {
                vertices_being_written = self.worker.vbw_cv.wait(vertices_being_written).await;
            }

            vertices_being_written.insert(self_id);
            drop(vertices_being_written);

            // now we have passed the filter

            let mut_self = self as *const Self as *mut Self;
            unsafe {
                old_val = (*mut_self).data.take();
                (*mut_self).data = Some(data)
            };

            // let others know I am done
            let mut vertices_being_written: MutexGuard<HashSet<VertexID>> =
                self.worker.vertices_being_written.lock().await;
            vertices_being_written.remove(&self_id);
            self.worker.vbw_cv.notify_one();

            old_val
        }
    }
}

pub struct RemoteVertex<T: DeserializeOwned + Serialize + fmt::Debug, V> {
    location: MachineID,
    worker: Arc<Worker<T, V>>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned + Serialize + fmt::Debug, V> fmt::Debug for RemoteVertex<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteVertex")
            .field("location", &self.location)
            .finish()
    }
}
/*
   Remote References to other vertices
*/
impl<T: DeserializeOwned + Serialize + fmt::Debug, V> RemoteVertex<T, V> {
    /*
       Constructor
    */
    pub fn new(location: MachineID, worker: Arc<Worker<T, V>>) -> Self {
        Self {
            location,
            worker,
            _marker: PhantomData,
        }
    }

    /*
       RPC for execute
    */
    async fn remote_execute<U: Serialize + DeserializeOwned>(
        &self,
        vertex_id: VertexID,
        auxiliary_information: U,
    ) -> V
    where
        T: DeserializeOwned + Serialize,
    {
        // The remote machine executes the function and returns the result.

        // Step 1: Construct channels and id
        let (tx, mut rx) = mpsc::channel::<V>(1000);
        let id = Uuid::new_v4();

        // Step 2: Add id to the worker's (id -> sending channel) mapping
        self.worker
            .result_multiplexing_channels
            .write()
            .await
            .insert(id, Mutex::new(tx));

        // Step 3: get lock on the sending stream so that all messages are sent in order, as expected
        //      (using the same rpc stream, send command and the data if necessary)
        let rpc_sending_streams = self.worker.rpc_sending_streams.read().await;
        let mut rpc_sending_stream = rpc_sending_streams
            .get(&self.location)
            .unwrap()
            .lock()
            .await;

        // Step 4: Construct the aux_info byte array, the rpc command with aux_info len
        let aux_info = bincode::serialize(&auxiliary_information).unwrap();
        let aux_info_len = aux_info.len();
        let command = bincode::serialize(&rpc::RPC::Execute(id, vertex_id, aux_info_len)).unwrap();

        // Step 5: Send the RPC Command and auxiliary information
        rpc_sending_stream
            .write_all(&[command, aux_info].concat())
            .await
            .unwrap();

        // Step 6: Drop the sender before waiting/blocking/yielding
        drop(rpc_sending_stream);
        drop(rpc_sending_streams);

        // Step 7: Wait on the receiver and return result
        let rpc_result = rx.recv().await.unwrap();
        rpc_result
    }
}

/*
   Enum to distinguish between different vertex kinds, for graph construction
*/
pub enum VertexKind {
    Local,
    Remote,
    Borrowed,
}
