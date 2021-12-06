# FoundationFS - File System Layer over FoundationDB

## Vision and Goals Of The Project
FoundationDB is a distributed key-store with strong ACID guarantees (Atomicity, Consistency, Isolation, Distributed). The core vision is to build a filesystem layer on top of FoundationDB that can leverage these internal consistencies to create a reliable (yet slow) distributed file store.

Using Linux’s Filesystem in Userspace (FUSE) module, our software will enable mounting the filesystem and interacting with it like a normal Unix directory, with our software behind the scenes translating reads and writes to FoundationDB transactions.

Each individual key retrieval or update has the potential to be slow. In order for advanced filesystem operations such as renaming and hard links to work, we will need layers of indirection that will require each operation to take multiple key retrievals. Because of this, we will be optimizing our design for functionality and correctness first, speed second. Designing an appropriate mapping of a file/directory structure into key-value storage is the most significant aspect of this project.

By running additional instances of FoundationDB, our software layer should scale without any changes. If our filesystem design is successful, this open source project will hopefully be a useful layer that can be shared with the larger FoundationDB community.

## Users/Personas Of The Project

FoundationDB is designed for users who favor Consistency, Distribution, Scalability, Security of data storage (e.g. financial transactions) over performance / speed. This project intends to add a distributed file system layer using key-value pair mapping to allow client applications to perform file / directory operations in a uniform way. For instance, users can perform *read* and *write* operations to a file, while more complex operations like *links* or *rename* might be implemented at the end. 

We could see this filesystem being mounted either by desktop users sharing resources, or by servers accessing shared datacenter storage.

---

## Scope and Features Of The Project

Support common file system functionalities such as read, write, move, rename and delete. Hard and symbolic link support will serve as interesting stretch goals.

To our users, our system should appear like any other mounted drive. Under the hood, we will have designed a consistent method for mapping filesystem operations to key-value updates and reads.

Operations performed on the File System could be slow due to stable architecture provided by FoundationDB

Provide a test suite for Client File system that is resilient to failures in a distributed setting

---

## Solution Concept

We will be using Java to develop a client layout that bridges the FUSE and FoundationDB APIs.


The actual method we use to map a file/directory structure over a key value store will require time and resources to design, and we consider it the primary challenge of the project. We will explore the methodologies used by existing key-value filesystems (such as kvefs), mimicking an ext-style inode tree, as well as explore variations on naive approaches such as simply having the file path be the key used in the store.


The end result should have an architecture similar to the following:

![Image of Diagram](Architecture.png)

---

## Acceptance criteria

- Design a mapping between a files and directories structure to key-value
- The ability to *read*, *write*, *open*, *close*, and *rename*, both files and directories from our Java layer.
- Being able to mount our application as a FUSE directory in Linux
  + Stretch goals
  + Hard links
  + Soft links
  + Permission system (*chmod*)
- Test suite ensures File system is resilient to random failures and concurrency issues

---

## Release Planning

We will attempt to deliver our product in the following stages of functionality:
1. Small Java client that can *read* and *write* key-value pairs to a FoundationDB cluster
2. A complete spec for our key-value filesystem mapping
3. Ability to perform *read*, *write*, *rmdir*, and *mkdir* operations to our Java layer
4. FUSE API integration for simple file/directory operations
5. Implement stretch features to Java client & FUSE integration 

---
## General comments

FoundationDB is a NoSQL database armed with ACID property. Using FoundationDB as the underlying storage for a file system offers many advantages, including strong security, high reliability, and easy-to-scale. Our file system layer targets customers who prefer their system to be reliable or when reliability and security is the first concern.

FUSE is a Linux kernel module used to mount userspace programs as Unix-compatible filesystems. The reference implementation can be found at [here](https://github.com/libfuse/libfuse). We plan on using an open source set of Java bindings found at [here](https://github.com/SerCeMan/jnr-fuse).
