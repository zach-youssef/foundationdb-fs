# FoundationFS - File System Layer over FoundationDB

[Final Demo](https://youtu.be/Q0pc37MOsxI)

## Prevous Demos

[Demo 1 Video](https://www.youtube.com/watch?v=zTL-7-rizSc)

[Demo 2 Video](https://youtu.be/6cjEmFCT3UU)

[Demo 3 Video](https://www.youtube.com/watch?v=KxgTht2I9VA)

[Demo 4 Video](https://youtu.be/i8d1wwhEPlw)

[Demo 5 Video](https://youtu.be/e5NDAjlDbDw)

## Vision and Goals Of The Project
FoundationDB is a distributed key-store with strong ACID guarantees (Atomicity, Consistency, Isolation, Distributed). The core vision is to build a filesystem layer on top of FoundationDB that can leverage these internal consistencies to create a reliable (yet slow) distributed file store.

Using Linux’s Filesystem in Userspace (FUSE) module, our software will enable mounting the filesystem and interacting with it like a normal Unix directory, with our software behind the scenes translating reads and writes to FoundationDB transactions.

Each individual key retrieval or update has the potential to be slow. In order for advanced filesystem operations such as renaming and hard links to work, we will need layers of indirection that will require each operation to take multiple key retrievals. Because of this, we will be optimizing our design for functionality and correctness first, speed second. Designing an appropriate mapping of a file/directory structure into key-value storage is the most significant aspect of this project.

By running additional instances of FoundationDB, our software layer should scale without any changes. If our filesystem design is successful, this open source project will hopefully be a useful layer that can be shared with the larger FoundationDB community.

Our original project propsal can be found [here](proposal.md).

## Installation and Setup 

### Notice for Windows Users

While Windows can be used to run FDB server instances, our client does not support non-unix operating systems, and will not work on Windows 
(or in Windows Subsystem for Linux, which at this time does not support FUSE).

### FDB Configuration

Full instructions for downloading the FDB client and server are available for [Linux](https://apple.github.io/foundationdb/getting-started-linux.html) and [MacOS](https://apple.github.io/foundationdb/getting-started-mac.html). 

For the client devices running the fslayer, you will only need to install `fdbclient`. Likewise, the machines hositng the database will only need `fdbserver`. 

Both clients and servers will need to have a [cluster file](https://apple.github.io/foundationdb/administration.html#cluster-files) configured.

Once the cluster files are configured, each [server can be started](https://apple.github.io/foundationdb/administration.html#starting-and-stopping) with your system daemon of choice, or with the `fdbserver` command.

### Fuse Installation (Mac Client Only)
(Linux users with an up-to-date kernel should not need to perform this step)

To run the fslayer client on a MacOS machine, you will need to download and install [macFUSE](https://github.com/osxfuse/osxfuse/releases).

### Building the client

From the `fslayer` directory, you can run

```
make build
```
or
```
./gradlew installDist
```

to install the application to `fslayer/app/build/install/app`. You can then copy the bin & lib contents to your root directory, if you are root, or run the application from there.

Alternatively, you can produce a zip distribution with
```
make zip
```
or
```
./gradlew distZip
```
which will be output to `fslayer/app/build/distributions`.

## Running the FoundationFS Client

To run the client and mount the FoundationDB Filesystem, run

```
fslayer <mount-path>
```
with the only argument being the directory you wish to mount the filesystem to.

### Logging in

You will be prompted for a username and password. If this is your first time, you can enter any username and password and the database will record that as your login information. On subsequent logins, you can use the same username & password combination.

## Testing

We have run FoundationFS against a small suite of filesystem tests. Documentation for those results can be found [here](https://docs.google.com/document/d/1wYN_tJ4bQFcuGFnopmGAmCfo3AxpNYdT-SbvnkJ2kFU/edit#)

If your FDB client and server are set up, you can run [cache-test.bash](testing/cache-test.bash) to verify that the cache versioning holds up when there are 100s of concurrent transactions.

```
testing/cache-test.bash
```

## How does it work?

Our filesystem client leverages FoundationDB's ACID guaruntees (Atomicity, Consistency, Isolation, Durability) to provide a stable and consistent distributed filesystem.

Filesystem operations are passed to our client through use of Unix's Filesystem in USErspace (FUSE) functionality. These operations are then converted into key-value operations to access or modify the data stored in FoundationDB.

![Image of Diagram](Architecture.png)

### Key-Value Schema

We use FoundationDB's DirectoryLayer extensively to create and manage unique key prefixes for each file and directory stored.
A detailed spec for the Java DirectoryLayer implementation can be found [here](https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/directory/DirectoryLayer.html);

In our schema, a file is a DirectorySubspace. Each fixed-size chunk of data is stored with the key prefix generated from `<path-to-file>/CHUNKS/<index>`.
Metadata such as `mode`, `uid`, and `m_time` are stored as keys with the file's subspace prefix.

![image](https://user-images.githubusercontent.com/10442582/144931380-057dc574-814c-4b39-aacb-6f66cf2676d9.png)

Directories are also DirectorySubspaces. Their metadata is stored with the subspace prefix `<path-to-dir>/.`. The presence of the `.` subspace distinguishes directories from files. 

![image](https://user-images.githubusercontent.com/10442582/144931406-90a98d60-84eb-4dc3-a05f-ec505aa4d06a.png)

This schema allows us to easily list a directory's contents by grabbing all child prefixes, grab all the file data from loading the keyrange of the chunk subspace, and quickly access a file or directory's information from their path.

### Client Caching

A `VERSION` is a counter stored in every file or directory's metadata as its own key-value pair. Everytime a file or its metadata is modified, that version is increased by one. Whenever a directory's metadata is changed, or a file/directory is added or removed to it, it's version increments as well.

![image](https://user-images.githubusercontent.com/10442582/144931435-d9ba2b7b-95d8-466b-9e52-6a8ec3c10bcc.png)

On a succesful read of a file or directory's contents, the client will cache the data, along with the `VERSION` of that file or directory.

On subsequent reads, the client will compare the cached version of a file or directory to the value in the database, and update it's cache if they do not match. Because of FoundationDB's gaurunteed consistency and atomicity, we know that by checking this version we will always be viewing the most current state of the filesystem.

### Unix Permissions

Each user that logs into the client gets assigned a UID starting at 70001 for operations on the database. This is the id that will be used to evaluate ownership and permissions on files. The ID for a user will be displayed in the console after a succesful login.

Please note that while files and directories might still display group permissions, group membership is currently not supported and will not be evaluated when determining if a user has access to a file operation. All users other than a file's owner will be evaluated using the "other" permission mode.

#### Permission data schema

The database's mappings from username to userID are stored in the subspace `./IDMAP/<username>`, 
while a user's PBKDF2 password hash is stored in  `./AUTH/<username>`.

![image](https://user-images.githubusercontent.com/10442582/144931480-1d8bf8cc-d506-4469-90ef-d863cf8bb3c5.png)

The key `./ID_COUNTER` stores the counter used to generate new unique UIDs.

![image](https://user-images.githubusercontent.com/10442582/144931508-4ed1466c-966f-4b55-9ad7-a8ac5b4410b8.png)

### Code Pointers

The entry point for the application is [App.java](fslayer/app/src/main/java/foundationdb_fslayer/App.java).

Here, you can see it create our wrapper around FDB, call our login manager, and pass both objects to our Fuse wrapper, which is then mounted.

#### PermissionManager

The [PermissionManager](fslayer/app/src/main/java/foundationdb_fslayer/permissions/PermissionManager.java) class handles password validation & storage, as well as loading user id mappings from the database.

#### FuseLayer

The [FuseLayer](fslayer/app/src/main/java/foundationdb_fslayer/fuse/FuseLayer.java) class implements the FuseStubFS interface provided by `jnr-fuse`. It translates the information from system calls into arguments pased to our FoundationDB operations, then parses that result into what the system expects.

#### FoundationLayer

[FoundationLayer.java](fslayer/app/src/main/java/foundationdb_fslayer/fdb/FoundationLayer.java) implements our 
[FoundationFileOperations.java](fslayer/app/src/main/java/foundationdb_fslayer/fdb/FoundationFileOperations.java) interface.

This class stores a reference to the actual FoundationDB java object and makes db transactions to perform system filesystem calls.

In many of its methods, it determines if a path is a directory or file, then instantiates and delegates the system operation to an object representing the filesystem object in question.

##### FileSchema & DirectorySchema

[FileSchema](fslayer/app/src/main/java/foundationdb_fslayer/fdb/object/FileSchema.java) and [DirectorySchema](fslayer/app/src/main/java/foundationdb_fslayer/fdb/object/DirectorySchema.java) represent file objects at a given path. Their methods take in a reference to the `DirectoryLayer` and a database transaction, then use these to read or modify the necessary keys to perform the file operation.
