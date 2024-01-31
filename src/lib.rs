/* lib.rs

   Amalgamation of imports, to keep everything under the same crate root.
   Also includes all information users should need when constructing the custom function

   Author: Binghong(Leo) Li
   Creation Date: 1/14/2024
*/

use core::fmt;

use crate::datastore::DataStore;
use crate::vertex::Vertex;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub mod datastore;
pub mod rpc;
pub mod udf;
pub mod vertex;
pub mod worker;

/*
   Trait requirement for user-defined functions
*/
#[async_trait]
pub trait UserDefinedFunction<
    T: DeserializeOwned + Serialize + fmt::Debug + Default,
    U: DeserializeOwned + Serialize,
    V,
>: Clone
{
    async fn execute(
        &self,
        vertex: &Vertex<T, V>,
        data_store: &DataStore<T, V>,
        auxiliary_information: U,
    ) -> V;
}
