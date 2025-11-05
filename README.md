# Node Migration Engine

The **Node Migration Engine** performs one-way migrations from a *source node tree* to a *destination node tree*.
It is **not** a two-way sync engine; implementing bidirectional sync would require a completely different algorithm.

---

## How It Works

Traversal is based on the **Breadth-First Search (BFS)** algorithm, chosen for its predictability and ability to coordinate two trees in lockstep.
Each traversal operation is isolated into discrete, non-recursive **tasks** so that workloads can be parallelized and queued efficiently.

### Task Flow

1. **List Children**
   From the current node, list immediate children only (no recursion).
   Identify each child’s type—whether it can contain further nodes (*recursive nodes*) or represents a terminal node.

2. **Apply Filters**
   Run each child through the configured filter rules using its ID, path, and context.
   Nodes that fail a rule are recorded along with the failure reason but are not scheduled for further traversal.
   This keeps each task stateless and lightweight.

3. **Record Results**
   Children that pass filtering are written immediately to the database (write-ahead log).
   Task propagation between queues happens entirely in-memory via the RoundQueue system for deterministic coordination.

---

## Why BFS Instead of DFS?

Depth-First Search (DFS) is memory-efficient, but it’s less suited to managing two trees in parallel.
BFS, while it requires storing all nodes at the current level, provides better control, checkpointing, and fault recovery.
The Node Migration Engine (NME) serializes traversal data to a local database after each round, keeping memory use bounded while preserving full traversal context.

### Two Possible Strategies

#### 1. Coupled DFS-Style Traversal

* The source and destination trees are traversed simultaneously.
* Each source node’s children are compared directly with the destination’s corresponding node.
* Throughput is limited by the slower of the two systems (e.g., 1000 nodes/s source vs 10 nodes/s destination).
* Harder to resume after interruptions because state exists only in memory.
* Example of this pattern: **Rclone**.
* Efficient but fragile for long migrations.

#### 2. Dual-Tree BFS Traversal (Our Approach)

* Source and destination are traversed **in rounds**.
* **Round 0**: traverse the source root and list its children.
* **Round 1**: traverse those children; destination traversal remains coordinated behind the source.
* The destination queue is coordinated to stay at least 3 rounds behind the source via the `QueueCoordinator`.
* When the destination processes its corresponding level, it compares existing nodes against the expected list from the source.
* Extra items in the destination are logged but not traversed further.
* The destination can run as fast as possible while staying coordinated with the source.
* Because each round is batched and stored, the system can resume exactly where it left off after a crash.
* This maximizes both safety and throughput.

---

## Two-Pass Design

The Node Migration Engine operates in two passes:

1. **Discovery Phase** – Traverses both node trees to identify what exists, what’s missing, and what conflicts may occur.
2. **Execution Phase** – After user review and approval, performs the actual creation or transfer of missing nodes.

This design gives users complete visibility and control before any data movement occurs.
