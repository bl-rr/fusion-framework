/* lib.rs
   Amalgamation of imports, to keep everything under the same crate root.
   Also includes all information users should need when constructing the custom function

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2023
*/

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::vertex::Vertex;
use crate::worker::Worker;

pub mod rpc;
pub mod udf;
pub mod vertex;
pub mod worker;

/*
   Trait requirement for user-defined functions
*/
#[async_trait]
pub trait UserDefinedFunction<T: DeserializeOwned + Serialize, U: DeserializeOwned + Serialize, V> {
    async fn execute(
        &self,
        vertex: &Vertex<T>,
        worker: &Worker<T, V>,
        auxiliary_information: U,
    ) -> V;
}
