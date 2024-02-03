/* rpc.rs

   Contains all the RPC related structs, defines the communication between nodes

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2024
*/

use core::fmt::Debug;

use crate::vertex::{Data, MachineID, VertexID};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

/*
    The initial RPC Communication
*/
#[derive(Serialize, Deserialize)]
pub enum RPC {
    // the number of fields may keep on growing as we need to pass more information to accommodate
    // for other types of execution
    Execute(Uuid, VertexID, usize), // usize for trailing data size
    Relay(Uuid, VertexID, usize),
    RequestData(Uuid, VertexID, usize),
    ExecuteWithData(Uuid, VertexID, usize),
    Update(Uuid, VertexID, usize),
}

/*
    Subsequent commands that send data
*/
#[derive(Serialize)]
pub struct RPCRelay<T: Serialize + DeserializeOwned + Default> {
    machine_id: MachineID,
    rpc_data: RPCData<T>,
}

/*
   Data
       the vertex/vertices to be sent
*/
#[derive(Serialize)]
pub struct RPCData<T: Serialize + DeserializeOwned + Default> {
    id: VertexID,
    data: Data<T>,
}

// communication/session control
#[derive(Serialize, Deserialize)]
pub struct RPCResponseHeader {
    pub session_id: Uuid,
    pub session_type: ResType,
    pub data_len: usize,
}

#[derive(Serialize, Deserialize)]
pub enum ResType {
    ExecuteRes,   // conveys data
    UpdateRes,    // Ack
    NotYetNeeded, // Note: for later use
}

//
#[derive(Serialize, Deserialize, Debug)]
pub enum RPCResPayload<T: Default, V: Debug> {
    ExecuteResPayload(V),
    UpdateResPayload(Option<Data<T>>),
}
