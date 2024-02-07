/* vertex.rs

   Contains all the vertex related structs and functions, a layer on top of Vanilla Data

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2024
*/
use core::cell::UnsafeCell;
use core::fmt::{self, Debug};

use crate::datastore::DataStore;
use crate::rpc::{RPCResPayload, RPC};
use crate::{worker::Worker, UserDefinedFunction};

use hashbrown::hash_map::Entry;
use hashbrown::{HashMap, HashSet};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::thread;
use std::thread::ThreadId;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex};
use tokio_condvar::Condvar;
use uuid::Uuid;

/* *********** Type Aliases *********** */
pub type VertexID = u32;
pub type MachineID = u32;

/* *********** struct definitions *********** */

/*
   Data Wrapper
*/
#[derive(Serialize, Debug, Clone, Default, Deserialize)]
pub struct Data<T: Default>(pub T);

/* VertexType
   A vertex is either
        1)  local:      local data
        2)  remote:     remote reference of vertex that lives on another machine/core/node
        3)  borrowed:   brought to local, original copy resides in remote (protected when leased?)
*/
#[derive(Debug)]
pub enum VertexType<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> {
    Local(LocalVertex<T, V>),
    Remote(RemoteVertex<T, V>),
    Borrowed(LocalVertex<T, V>),
    // Note: maybe a (Leased) variant for the future?
}

/*
   Vertex
*/
#[derive(Debug)]
pub struct Vertex<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> {
    pub id: VertexID,
    pub v_type: VertexType<T, V>,
}
impl<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> Vertex<T, V> {
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
    pub async fn get_val(&self) -> SafeDataReference<T, V> {
        match &self.v_type {
            VertexType::Local(local_v) | VertexType::Borrowed(local_v) => local_v.get_data().await,
            VertexType::Remote(_) => {
                // this should never be reached

                // sure one could do this, but you really shouldn't
                panic!("Remote Node should not invoke get_val() function")
            }
        }
    }
    pub async fn update(&self, data: Data<T>) -> Option<Data<T>> {
        match &self.v_type {
            VertexType::Local(local_v) | VertexType::Borrowed(local_v) => {
                local_v.set_data(data).await
            }
            VertexType::Remote(remote_v) => {
                // TODO: add some meaningful return later
                remote_v.remote_update(data, self.id).await
            }
        }
    }
}

/*
   Vertex that resides locally, or borrowed to be temporarily locally
*/
pub struct LocalVertex<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> {
    incoming_edges: HashSet<VertexID>, // for simulating trees, or DAGs
    outgoing_edges: HashSet<VertexID>, // for simulating trees, or DAGs
    edges: HashSet<VertexID>,          // for simulating general graphs
    data: Arc<MyUnsafeCell<Option<Data<T>>>>, // Using option to return the previous value (for error checking, etc.)
    borrowed_in: bool,                        // When a node is a borrowed node
    leased_out: bool,                         // When the current node is lent out
    vertex_lock: Mutex<VertexAccessor>,
    vertex_lock_cv: Condvar,
    _marker: PhantomData<V>,
}

impl<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> Debug for LocalVertex<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalVertex")
            .field("incoming_edges", &self.incoming_edges)
            .field("outgoing_edges", &self.outgoing_edges)
            .field("edges", &self.edges)
            .field("data", unsafe { &*self.data.0.get() })
            .field("accessor", &self.vertex_lock)
            .finish()
    }
}

impl<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> LocalVertex<T, V> {
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
            data: Arc::new(MyUnsafeCell::new(data)),
            borrowed_in: false,
            leased_out: false,
            vertex_lock: Mutex::new(VertexAccessor::default()),
            vertex_lock_cv: Condvar::new(),
            _marker: PhantomData,
        }
    }

    /*
       Builder/Creator method for easier construction in graph constructors
       or in general when creating individual vertices
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
    pub async fn get_data(&self) -> SafeDataReference<T, V> {
        // this basically means that no one is writing
        let mut accessor = self.vertex_lock.lock().await;

        // now it's either free or read
        if matches!(accessor.state, VertexState::Free) {
            accessor.state = VertexState::Read;
        }

        // insert the current thread_id into the reading
        *accessor.reading.entry(thread::current().id()).or_insert(0) += 1;

        SafeDataReference {
            data: Some(unsafe { &*self.data.0.get() }),
            parent: Some(self),
        }
    }
    pub async fn set_data(&self, data: Data<T>) -> Option<Data<T>> {
        if self.leased_out {
            None
        } else {
            let old_val;

            let mut accessor = self.vertex_lock.lock().await;

            loop {
                match accessor.state {
                    VertexState::Free => {
                        // I can write
                        break;
                    }
                    VertexState::Read => {
                        let reading = &accessor.reading;
                        if !(reading.len() == 1
                            && reading.keys().next().unwrap().eq(&thread::current().id()))
                        {
                            accessor = self.vertex_lock_cv.wait(accessor).await;
                        } else {
                            // the current thread is the only reader
                            break;
                        }
                    }
                }
            }

            // Note: The CondVar is not "Cancellation Safe", yet CondVar would be the most appropriate construct here

            // now we have passed the filter
            unsafe {
                old_val = self.data.get().replace(data);
            };

            old_val
        }
    }
}

pub struct RemoteVertex<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> {
    location: MachineID,
    worker: Arc<Worker<T, V>>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> Debug for RemoteVertex<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteVertex")
            .field("location", &self.location)
            .finish()
    }
}
/*
   Remote References to other vertices
*/
impl<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> RemoteVertex<T, V> {
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

    // TODO: should all of these be non-blocking? In the sense that within the udf they can proceed without waiting for a response?

    /*
       RPC for execute
    */
    async fn remote_execute<U: Serialize + DeserializeOwned>(
        &self,
        vertex_id: VertexID,
        auxiliary_information: U,
    ) -> V {
        // The remote machine executes the function and returns the result.

        // Step 1: Construct channels and id
        let (tx, mut rx) = mpsc::channel::<RPCResPayload<_, V>>(1000);
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
        let command = bincode::serialize(&RPC::Execute(id, vertex_id, aux_info_len)).unwrap();

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
        return match rpc_result {
            RPCResPayload::ExecuteResPayload(res) => res,
            other => {
                panic!(
                    "received other rpc payload than execute response: {:?}",
                    other
                )
            }
        };
    }

    /*
       RPC for update
    */
    // Note: maybe refactor later for DRY principle
    async fn remote_update(&self, data: Data<T>, v_id: VertexID) -> Option<Data<T>> {
        // Step 1: Construct channels and id
        let (tx, mut rx) = mpsc::channel::<RPCResPayload<T, _>>(1000);
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

        // Step 4: Construct the data byte array, the rpc command with data len
        let data_bytes = bincode::serialize(&data).unwrap();
        let size = data_bytes.len();
        let command = bincode::serialize(&RPC::Update(id, v_id, size)).unwrap();

        // Step 5: Send the RPC Command and auxiliary information
        rpc_sending_stream
            .write_all(&[command, data_bytes].concat())
            .await
            .unwrap();

        // Step 6: Drop the sender before waiting/blocking/yielding
        drop(rpc_sending_stream);
        drop(rpc_sending_streams);

        // Step 7: Wait on the receiver and return result
        let rpc_result = rx.recv().await.unwrap();
        return match rpc_result {
            RPCResPayload::UpdateResPayload(res) => res,
            other => {
                panic!(
                    "received other rpc payload than update response: {:?}",
                    other
                )
            }
        };
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

/*
   New as of 02/06/2024

   Implementing the logic to return a reference whose lifetime is "checked"/guaranteed in runtime.

*/

#[derive(Debug)]
struct MyUnsafeCell<T>(UnsafeCell<T>);

unsafe impl<T> Sync for MyUnsafeCell<T> {}

impl<T> MyUnsafeCell<T> {
    fn new(data: T) -> MyUnsafeCell<T> {
        MyUnsafeCell(UnsafeCell::new(data))
    }

    unsafe fn get(&self) -> &mut T {
        &mut *self.0.get()
    }
}

pub struct SafeDataReference<'a, 'b, T: DeserializeOwned + Serialize + Debug + Default, V: Debug> {
    data: Option<&'a Option<Data<T>>>,
    parent: Option<&'b LocalVertex<T, V>>,
}

// Implement the Deref trait for MyType
impl<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> Deref
    for SafeDataReference<'_, '_, T, V>
{
    type Target = Option<Data<T>>;

    // Implement the dereference function
    fn deref(&self) -> &Self::Target {
        &self.data.unwrap() // Return a reference to the inner value
    }
}

impl<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> Drop
    for SafeDataReference<'_, '_, T, V>
{
    fn drop(&mut self) {
        futures::executor::block_on(self.async_drop());
    }
}

impl<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> Default
    for SafeDataReference<'_, '_, T, V>
{
    fn default() -> Self {
        Self {
            data: None,
            parent: None,
        }
    }
}

impl<T: DeserializeOwned + Serialize + Debug + Default, V: Debug> SafeDataReference<'_, '_, T, V> {
    async fn async_drop(&mut self) {
        println!("dropping!");
        let mut accessor = self.parent.unwrap().vertex_lock.lock().await;
        match accessor.reading.entry(thread::current().id()) {
            Entry::Occupied(entry) => {
                if *entry.get() <= 1 {
                    // If the count is 1 or less, remove the key
                    entry.remove();

                    // If no more keys, reset the state
                    if accessor.reading.is_empty() {
                        accessor.state = VertexState::Free;
                    }
                } else {
                    // Otherwise, decrement the count by 1
                    *entry.into_mut() -= 1;
                }
            }
            Entry::Vacant(_) => {
                // If the key does not exist, there's nothing to decrement
                panic!("An entry should exist!")
            }
        };
        println!("Nice dropping!");
        self.parent.unwrap().vertex_lock_cv.notify_all();
    }
}

#[derive(Default, Debug)]
pub struct VertexAccessor {
    state: VertexState,
    reading: HashMap<ThreadId, usize>,
}

#[derive(Default, Debug)]
pub enum VertexState {
    #[default]
    Free,
    Read,
}
