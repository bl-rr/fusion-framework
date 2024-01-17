/* lib.rs
   Amalgamation of imports, to keep everything under the same crate root.
   Also includes all information users should need when constructing the custom function

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2023
*/

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

use crate::graph::Graph;
use crate::vertex::Vertex;

pub mod graph;
pub mod rpc;
pub mod udf;
pub mod vertex;

/*
   Trait requirement for user-defined functions
*/
#[async_trait]
pub trait UserDefinedFunction<
    T: DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize + Debug,
>
{
    async fn execute(&self, vertex: &Vertex<T>, graph: &Graph<T, U>, auxiliary_information: U)
        -> T;
}
