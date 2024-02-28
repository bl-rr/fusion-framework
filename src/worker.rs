/* worker.rs

   Contains the primitives for inter-node communication, as well as some intra-node data update integrity

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2024
   Major Revision: 1/30/2024
*/

use alloc::sync::Arc;
use core::fmt::Debug;

use crate::rpc::RPCResPayload;
use crate::vertex::*;

use hashbrown::HashMap;
use serde::{de::DeserializeOwned, Serialize};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

/*
    Worker Struct that stores the (vertex_id -> vertex) mapping, acting as pointers to vertices
        as well as the communication channels

    TODO: Add weights to edges
*/
pub struct Worker<T: DeserializeOwned + Serialize + Default, V: Debug> {
    // pub graph: HashMap<VertexID, Vertex<T>>, // vertex_id -> vertex mapping
    pub sending_streams: Arc<HashMap<MachineID, Arc<TcpStream>>>,
    pub rpc_sending_streams: Arc<HashMap<MachineID, Arc<TcpStream>>>,
    pub result_multiplexing_channels: RwLock<HashMap<Uuid, Mutex<Sender<RPCResPayload<T, V>>>>>, // Note: maybe make the result either (V, or Data<T>, or T)
}

impl<T: DeserializeOwned + Serialize + Default, V: Debug> Worker<T, V> {
    /*
       Constructor
    */
    pub fn new() -> Self {
        Worker {
            sending_streams: Arc::new(HashMap::new()),
            rpc_sending_streams: Arc::new(HashMap::new()),
            result_multiplexing_channels: RwLock::new(HashMap::new()),
        }
    }
}
