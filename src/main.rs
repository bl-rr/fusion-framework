use std::collections::HashMap;
use std::sync::Arc;
use tokio;
use tokio::sync::Mutex;

// Type aliases for clarity
type DataID = u32;
type MachineID = u32;
type Data = Vec<u8>;
type Result = Vec<u8>;

// A structure representing a data object with an ID and content
pub struct DataObject {
    id: DataID,
    content: Data,
}

/******** User Custom ********/
// Dummy implementation of a function to calculate some result from data
fn user_calculate_f(data_ids: Vec<&Data>) -> Result {
    data_ids.iter().flat_map(|data| data.to_vec()).collect()
}

// Dummy implementation of a function to merge results
fn user_merge_f(results: Vec<Result>) -> Result {
    results.into_iter().flatten().collect()
}

// Constant representing the ID of the machine this code is running on
const SELF_MACHINE_ID: MachineID = 0;
/******** User Custom ********/

// The Worker struct, representing a node in the distributed system
pub struct Worker {
    local_store: HashMap<DataID, Data>, // Local storage of data
    remote_references: HashMap<DataID, MachineID>, // References to data stored remotely
    reference_store: Arc<Mutex<HashMap<DataID, Data>>>, // Store for references to remote data
    calculate_f: fn(Vec<&Data>) -> Result, // Function to calculate results from data
    merge_f: fn(Vec<Result>) -> Result, // Function to merge results
    self_machine_id: MachineID,         // ID of this machine
}

impl Worker {
    // Initialize a new Worker with given parameters
    pub fn init(
        local_store: HashMap<DataID, Vec<u8>>,
        remote_references: HashMap<DataID, MachineID>,
        calculate_f: fn(Vec<&Data>) -> Result,
        merge_f: fn(Vec<Result>) -> Result,
        self_machine_id: MachineID,
    ) -> Worker {
        Worker {
            local_store,
            remote_references,
            reference_store: Arc::new(Mutex::new(HashMap::new())),
            calculate_f,
            merge_f,
            self_machine_id,
        }
    }

    // Main calculation function, decides whether to execute locally or remotely
    async fn calculate(&mut self, data_ids: Vec<DataID>) -> Result {
        let target_machine_id = self.schedule(&data_ids);

        if target_machine_id.eq(&self.self_machine_id) {
            // If execution is local
            let non_local_ids: Vec<DataID> = data_ids
                .iter()
                .filter(|&id| !self.is_local(id))
                .cloned()
                .collect();

            // Retrieve non-local data
            for id in non_local_ids {
                self.rpc_receive(&self.get_data_location(&id), &id);
            }

            // Execute locally
            let res = self.execute(data_ids);
            self.reference_store.lock().await.clear(); // Note: may need to acknowledge the completion
            res.await
        } else {
            // If execution is remote
            data_ids.iter().for_each(|data_id| {
                if !self.is_local(data_id) {
                    let machine_id = self
                        .remote_references
                        .get(data_id)
                        .expect("DataID nonexistent in Remote")
                        .clone();
                    if machine_id.ne(&target_machine_id) {
                        self.rpc_relay(&machine_id, &data_id);
                    }
                } else {
                    self.rpc_send(
                        &target_machine_id,
                        DataObject {
                            id: data_id.clone(),
                            content: self.get_data(&data_id),
                        },
                    );
                }
            });

            // Execute on the remote machine
            self.rpc_execute(target_machine_id, data_ids).await
        }
    }

    // Function to merge results, currently a placeholder
    fn merge(f: fn(Vec<Result>) -> Result) -> Result {
        [255u8; 16].to_vec()
    }

    // Placeholder for remote procedure call to send data
    fn rpc_send(&self, machine_id: &MachineID, data_object: DataObject) {}

    // Placeholder for remote procedure call to relay data
    fn rpc_relay(&self, machine_id: &MachineID, data_id: &DataID) {}

    // Placeholder for remote procedure call to receive data
    fn rpc_receive(&mut self, machine_id: &MachineID, data_id: &DataID) {
        // adding incoming data to reference store
    }

    async fn receive_rpc(&self) {
        // Placeholder for RPC receiving logic
        // ...

        // Process the received RPC
        self.process_rpc().await;
    }

    async fn process_rpc(&self) {
        // Placeholder for RPC processing logic
        // ...

        // Process data in local_store
        // ...
    }

    // Placeholder for remote execution of a calculation
    async fn rpc_execute(&self, machine_id: MachineID, data_ids: Vec<DataID>) -> Result {
        // make sure
        [255u8; 16].to_vec()
    }

    // Execute the calculation on local data
    async fn execute(&self, data_ids: Vec<DataID>) -> Result {
        let rf_store = self.reference_store.lock().await;
        let mut data = vec![];
        for id in data_ids {
            data.push(
                self.local_store
                    .get(&id)
                    .unwrap_or(rf_store.get(&id).expect("data not yet ready")),
            );
        }
        (self.calculate_f)(data)
    }

    // Check if data is stored locally
    fn is_local(&self, id: &DataID) -> bool {
        self.local_store.contains_key(id)
    }

    // Retrieve data from the local store
    fn get_data(&self, id: &DataID) -> Data {
        self.local_store
            .get(id)
            .expect("DataID non existent locally")
            .clone()
    }

    // Retrieve the location (machine ID) where a data ID is stored
    fn get_data_location(&self, id: &DataID) -> MachineID {
        self.remote_references
            .get(id)
            .expect("DataID non existent remotely")
            .clone()
    }

    // Scheduling algorithm to decide where to process the data
    fn schedule(&self, data_ids: &Vec<DataID>) -> MachineID {
        let mut machine_count: HashMap<MachineID, usize> = HashMap::new();

        for id in data_ids {
            let machine_id = if self.is_local(id) {
                self.self_machine_id
            } else {
                *self.remote_references.get(id).expect("DataID non existent")
            };

            *machine_count.entry(machine_id).or_insert(0) += 1;
        }

        machine_count
            .into_iter()
            .max_by_key(|&(_, count)| count)
            .map_or(self.self_machine_id, |(machine_id, _)| machine_id)
    }
}

// Main function to initialize and test the Worker
fn main() {
    let mut local_store = HashMap::new();
    local_store.insert(0, [0u8; 16].to_vec()).unwrap();
    local_store.insert(1, [1u8; 16].to_vec()).unwrap();

    let mut remote_references = HashMap::new();
    remote_references.insert(0, 0).unwrap();
    remote_references.insert(1, 0).unwrap();
    remote_references.insert(2, 1).unwrap();
    remote_references.insert(3, 2).unwrap();

    // Initialize the Worker
    let worker = Worker::init(
        local_store,
        remote_references,
        user_calculate_f,
        user_merge_f,
        SELF_MACHINE_ID,
    );

    let worker = Arc::new(worker);

    let worker_clone = Arc::clone(&worker);
    tokio::spawn(async move {
        loop {
            worker_clone.receive_rpc().await;
        }
    });

    loop {
        // main loop, perform algorithm
    }
}
