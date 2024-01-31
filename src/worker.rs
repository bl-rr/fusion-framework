/* worker.rs

   Contains the primitives for inter-node communication, as well as some intra-node data update integrity

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2024
   Major Revision: 1/30/2024
*/

use crate::vertex::*;

use hashbrown::{HashMap, HashSet};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, RwLock};
use tokio_condvar::Condvar;
use uuid::Uuid;

/*
    Worker Struct that stores the (vertex_id -> vertex) mapping, acting as pointers to vertices
        as well as the communication channels

    TODO: Add weights to edges
*/
pub struct Worker<T: DeserializeOwned + Serialize, V> {
    // pub graph: HashMap<VertexID, Vertex<T>>, // vertex_id -> vertex mapping
    pub sending_streams: RwLock<HashMap<MachineID, Mutex<TcpStream>>>,
    pub rpc_sending_streams: RwLock<HashMap<MachineID, Mutex<TcpStream>>>,
    pub result_multiplexing_channels: RwLock<HashMap<Uuid, Mutex<Sender<V>>>>, // Note: maybe make the result either (V, or Data<T>, or T)
    pub vertices_being_written: Arc<Mutex<HashSet<VertexID>>>,
    pub tree_being_written: Arc<Mutex<bool>>,
    pub vbw_cv: Arc<Condvar>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned + Serialize, V> Worker<T, V> {
    /*
       Constructor
    */
    pub fn new() -> Self {
        Worker {
            sending_streams: RwLock::new(HashMap::new()),
            rpc_sending_streams: RwLock::new(HashMap::new()),
            result_multiplexing_channels: RwLock::new(HashMap::new()),
            vertices_being_written: Arc::new(Mutex::new(HashSet::new())),
            tree_being_written: Arc::new(Mutex::new(false)),
            vbw_cv: Arc::new(Condvar::new()),
            _marker: PhantomData,
        }
    }
}
