
# Design Document: Low-Latency Distributed Compute Framework

## Overview
This document outlines the design for a low-latency, distributed computing framework. The framework aims to minimize data movement, enable computation across multiple machines, and optimize for sub-microsecond latency.

## Components
1. **Vertex**: Represents a data object in each node. Defines computation procedures for different types of nodes.
2. **Worker**: Represents a compute node in the distributed system, holds all vertices as well as communication channels with other worker nodes.
3. **RPC Protocols**: Defines how one worker should communicate with another when needed.
4. **UDFs**: User defined functions that follow a specific trait requirements, allowing them to be run with the framework

## Key Features
1. **Distributed Data Management**:
   - Local Storage: Data stored locally in a HashMap.
   - Remote References: Nodes maintain mappings of data IDs to machine IDs for data located on other nodes.
   - Reference Store: When a node essentially takes ownership of data residing at another node.

2. **Computation Functions**:
   - User-defined functions for calculation and merging of data.

3. **Latency Optimization**:
   - Asynchronous Processing: Utilizes Rust's `tokio` and `async/await`.
   - Scheduling Algorithm: Determines the optimal machine for computation.

4. **Inter-Node Communication**:
   - RPC Mechanisms: Functions to simulate remote procedure calls.

## Operations
1. **Initialization**: Workers initialized with local and remote data mappings, computation functions, and machine ID.
2. **Data Processing**:
   - Calculation: Orchestrates task execution.

[//]: # (   - Merging: Combines results from various nodes.)

3. **RPC Handling**:
   - Send and receive data among nodes.
   - Process received RPCs asynchronously.

[//]: # (4. **Scheduling**:)

[//]: # (   - Decision-making on local vs. remote processing.)

## Scalability and Reliability
- Scales horizontally with more nodes.
- Ensures data integrity and resilience (at the minimal level, others decoupled).

[//]: # (## Future Enhancements)

[//]: # (1. **Optimization of RPC Mechanisms**)

[//]: # (2. **Enhanced Scheduling Algorithms**)

[//]: # (3. **Garbage Collection Optimization**)

## Codebase Overview
- **lib.rs**: Amalgamation of imports, structuring the crate, and defining user function requirements.
- **main.rs**: Testbed for the framework with initialization and processing examples.
- **graph.rs**: Graph-related structures and functions, layer over vertices.
- **rpc.rs**: RPC communication setup, including session headers and data types.
- **udf.rs**: User-defined functions and auxiliary information structures.
- **vertex.rs**: Vertex-related structures and functions, managing local, remote, and borrowed vertex types.

## Conclusion
This design document provides an in-depth look at the low-latency distributed compute framework, covering its architecture, components, features, and scalability. The framework is designed for efficient data processing across distributed systems while maintaining low latency and high reliability.

