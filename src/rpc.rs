/* rpc.rs
   Contains all the RPC related structs, defines the communication between nodes

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2023
*/

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

use crate::vertex::{MachineID, Vertex, VertexID};

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
pub struct RPCRelay<T: Serialize + DeserializeOwned> {
    machine_id: MachineID,
    rpc_data: RPCData<T>,
}

/*
   Data
       the vertex/vertices to be sent
*/
#[derive(Serialize)]
pub struct RPCData<T: Serialize + DeserializeOwned> {
    id: VertexID,
    data: Vertex<T>,
}

// communication/session control
#[derive(Serialize, Deserialize)]
pub struct SessionHeader {
    pub session_id: Uuid,
    pub session_type: DataType,
    pub data_len: usize,
}

#[derive(Serialize, Deserialize)]
pub enum DataType {
    Result,
    NotYetNeeded, // Note: for later use
}
