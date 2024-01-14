/* rpc.rs
   Contains all the RPC related structs, defines the communication between nodes

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2023
*/

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::vertex::{MachineID, Vertex, VertexID};

/* 
    The initial RPC Communication 
*/
#[derive(Serialize, Deserialize)]
pub enum RPC {
    Execute(VertexID),
    Relay(VertexID),
    RequestData(VertexID),
    ExecuteWithData(VertexID),
    Update(VertexID),
}

/*
    Subsequent commands that send data
*/
#[derive(Serialize)]
pub struct RPCRelay<T: Serialize + DeserializeOwned> {
    machine_id: MachineID,
    rpc_data: RPCData<T>
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


