
# Design Document: Low-Latency Distributed Compute Framework

## Overview
This document outlines the design for a low-latency, distributed computing framework. The framework aims to minimize data movement, enable computation across multiple machines, and optimize for sub-microsecond latency.

## Components
1. **Worker Structure**: Represents a node in the distributed system.
2. **DataObject**: Struct for sending data with unique identifiers.

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
    - Merging: Combines results from various nodes.

3. **RPC Handling**:
    - Send and receive data among nodes.
    - Process received RPCs asynchronously.

4. **Scheduling**:
    - Decision-making on local vs. remote processing.

## Scalability and Reliability
- Scales horizontally with more nodes.
- Ensures data integrity and resilience (at the minimal level, others decoupled).

## Future Enhancements
1. **Optimization of RPC Mechanisms**
2. **Enhanced Scheduling Algorithms**
3. **Garbage Collection Optimization**