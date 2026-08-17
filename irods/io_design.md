## System Topology and Core Philosophy

`[DE Pod (FUSE)]` ↔ *(gRPC / network boundary)* ↔ `[fs-proxy]` ↔ *(high-speed network)* ↔ `[iRODS]`

- **FUSE Client (DE Pod):** A lightweight gateway that separates communication strategies by file open mode to offset network latency (RTT).
- **fs-proxy (Middleware):** Combines a 100GB memory cache, local filesystem, and **BadgerDB** to absorb large-scale I/O load and overcome iRODS limitations.

---

## 1. FUSE Client (DE Pod) Design

The FUSE client completely separates its internal behavior based on the file open mode (flags).

### **RDONLY (Read-Only Mode)**

- **Goal:** Hide gRPC network latency and provide zero-RTT read responses.
- **Prefetching:** When the kernel requests a 64KB read, the client fetches 4MB blocks from `fs-proxy` via gRPC streaming. Streamed data is stored in a buffer and served immediately to read requests.
- **Double Buffering:** As a 4MB block is being consumed, the next 4MB block is requested in the background to guarantee seamless sequential read performance.

### **WRONLY (Write-Only Mode)**

- **Goal:** Reduce client memory usage and eliminate complex buffer management logic.
- **Micro Buffering:** Instead of waiting for a full 4MB, data is batched at 512KB–1MB to reduce CPU serialization overhead only.
- **gRPC Streaming:** Micro-buffered data is continuously pushed to `fs-proxy` via gRPC stream (no flush exception handling required).

### **RDWR (Read/Write Mixed and Random I/O)**

- **Goal:** Prevent read-after-write data corruption and cache inconsistency in environments with frequent offset jumps (e.g., database files).
- **Pass-through (No Buffering):** Prefetching and micro buffering are **completely disabled**.
- **Direct Communication:** All random reads/writes and `fsync()` requests from FUSE are forwarded directly to `fs-proxy` via gRPC stream without modification.

---

## 2. fs-proxy (Middleware) Design

Data content is stored as **files** on local disk, while synchronization state and metadata are strictly separated into **BadgerDB**.

### **RDONLY Handling (100GB Memory Cache)**

- **4MB Block Caching:** Data is stored in heap memory as 4MB chunks to minimize object count and avoid GC pressure.
- **TTL and LRU-based Invalidation:** Cache entries are evicted after a configurable TTL to reflect external changes (e.g., direct iRODS access), providing eventual consistency.

### **WRONLY Handling (Local File + BadgerDB Deferred Sync)**

- **Local File Write:** Data received via gRPC stream is written directly to a temporary file on the `fs-proxy` local disk (e.g., `/tmp/fs-proxy/data/{file_id}`), and a success response is sent to the client immediately.
- **BadgerDB State Recording:** Once the data file write completes, the file's metadata and state are recorded as `Dirty (needs iRODS sync)` in BadgerDB, a high-performance KV store.
- **Background Sync:** A background worker reads the `Dirty` queue from BadgerDB and uploads local files to iRODS. On success, the state is updated to `Clean`.

### **RDWR Handling (Working Copy-based Local I/O)**

- **Memory Cache Bypass:** When a file is opened in RDWR mode, the 100GB memory cache is entirely bypassed.
- **Working Copy Creation:** The file is downloaded from iRODS to local disk to create a working copy. (Working copy metadata is registered in BadgerDB.)
- **High-speed Local Processing:** All subsequent random read/write I/O from the client is served entirely from this local working copy file.
- **Final Merge:** When the file is `Close`d or becomes idle, it is enqueued for synchronization via BadgerDB and the final version is uploaded back to iRODS.

---

## 3. Crash Recovery via BadgerDB

By using BadgerDB as the metadata store, the primary weakness of deferred writes (data loss) is fully mitigated.

- Even if the `fs-proxy` server reboots unexpectedly or the daemon crashes, the actual data files written to disk and the BadgerDB state records are safely preserved.
- On `fs-proxy` restart, BadgerDB is scanned first to find files that were not marked `Clean`, and background iRODS upload jobs are automatically resumed.