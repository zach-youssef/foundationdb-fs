package foundationdb_fslayer.fdb.object;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import foundationdb_fslayer.Util;
import foundationdb_fslayer.cache.FileCacheEntry;
import foundationdb_fslayer.cache.FsCacheSingleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static foundationdb_fslayer.Util.parsePath;

public class FileSchema extends AbstractSchema {
    private final String rawPath;
    private final List<String> path;
    private final List<String> chunksPath;

    public final static int CHUNK_SIZE_BYTES = 1000;

    @Override
    protected String getPath() {
        return rawPath;
    }

    @Override
    protected DirectorySubspace getMetadataSpace(DirectoryLayer directoryLayer, ReadTransaction rt) {
        try {
            return directoryLayer.open(rt, path).get();
        } catch (Exception e) {
            throw new IllegalStateException("this dir dont exist");
        }
    }

    @Override
    protected String getVersionKey() {
        return Metadata.VERSION;
    }

    private static class Metadata {
        final static String CHUNKS = "CHUNKS";
        final static String TIMESTAMP = "TIMESTAMP";
        final static String MODE = "MODE";
        final static String USER = "UID";
        final static String GROUP = "GID";
        final static String VERSION = "VERSION";
    }

    public FileSchema(String path) {
        this.rawPath = path;
        this.path = parsePath(path);
        this.chunksPath = new ArrayList<>(this.path);
        this.chunksPath.add(Metadata.CHUNKS);
    }

    /**
     * Create the subspace to store this file
     * Returns false on error
     */
    public boolean create(DirectoryLayer dir, Transaction transaction) {
        try {
            // Create the subspace
            DirectorySubspace fileSpace = dir.create(transaction, path).get();
            // Create the subspace to store data chunks
            DirectorySubspace chunkSpace = dir.create(transaction, chunksPath).get();
            // Initialize empty first chunk
            transaction.set(chunkSpace.pack(0), new byte[0]);
            // Initialize Version counter
            transaction.set(fileSpace.pack(Metadata.VERSION), Tuple.from(0L).pack());
            // Invalidate the cache of the parent dir
            incrementParentVersion(dir, transaction);;

            return true;
        } catch (Exception e) {
            System.out.println("Failed to create file");
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Reads the current value of the file.
     * Will return null on error.
     */
    public byte[] read(DirectoryLayer dir, ReadTransaction transaction, long offset, long size, long userId) {
        if (!readPermitted(dir, transaction, userId)) {
            return null;
        }

        try {
            // Open the file's chunk space
            DirectorySubspace chunkSpace = dir.open(transaction, chunksPath).get();

            // Grab all the relevant chunks of the file
            int startChunk = (int) (offset / CHUNK_SIZE_BYTES);
            int endChunk = (int) ((offset + size) / CHUNK_SIZE_BYTES);
            List<byte[]> chunks = getCache(dir, transaction).getData(startChunk, endChunk);

            // Initialize buffer to store file
            byte[] data = new byte[(int) Math.min(this.size(dir, transaction) - offset, size)];
            int dataIndex = 0;

            for (int i = 0; i < chunks.size(); ++i) {
                int copyIndex = 0;
                if (i == 0) {
                    // If this is the first chunk, make sure we start at the offset
                    copyIndex += offset % CHUNK_SIZE_BYTES;
                }
                byte[] chunkData = chunks.get(i);
                // Copy the data from the chunk into the return buffer
                for (; copyIndex < chunkData.length && dataIndex < data.length; copyIndex++){
                    data[dataIndex++] = chunkData[copyIndex];
                }
            }

            return data;
        } catch (Exception e) {
            return null;
        }
    }

    /** Calculate total size of the file */
    public int size(DirectoryLayer dir, ReadTransaction transaction) {
        try {
            List<byte[]> chunks = getCache(dir, transaction).getData();
            return ((chunks.size() - 1) * CHUNK_SIZE_BYTES) + (chunks.get(chunks.size() - 1).length);
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Appends the given bytes to the file.
     * Returns false if an error occurs.
     */
    public boolean write(DirectoryLayer dir, Transaction transaction, byte[] data, long offset, long userId) {
        if (!modifyPermitted(dir, transaction, userId)) {
            return false;
        }

        try {
            DirectorySubspace chunkSpace = dir.open(transaction, chunksPath).get();

            // Grab existing chunks we will be writing to
            int startChunk = (int) (offset / CHUNK_SIZE_BYTES);
            int endChunk = (int) ((offset + data.length) / CHUNK_SIZE_BYTES);
            // Grab the chunks we are not completely overwriting
            byte[] startChunkData = getCache(dir, transaction).getData(startChunk);
            byte[] endChunkData = getCache(dir, transaction).getData(endChunk);

            // Keep track of what has been written
            int bytesWritten = 0;

            // I'm sorry for how gross this loop is.
            for (int chunkNum = startChunk; chunkNum <= endChunk; ++chunkNum) {
                // This chunk will either be completely filled or be given
                // the rest of the data
                int newBufferSize = (int) Math.min(data.length - bytesWritten
                        + (chunkNum == startChunk? offset : 0),
                        CHUNK_SIZE_BYTES);
                // If this is the last chunk, make room in the buffer
                // for the data in this chunk that comes after the new data
                if (chunkNum == endChunk) {
                    newBufferSize = Math.max(endChunkData.length, newBufferSize);
                }
                // Initialize the buffer we will write to the database
                byte[] newBuffer = new byte[newBufferSize];
                // This index keeps track of where we are writing in the buffer
                int newBufferIndex = 0;
                // If this is the start chunk, we need to copy the old data up to where the offset actually
                // begins
                if (chunkNum == startChunk) {
                    // Copy data from the start buffer up to the offset
                    for (; newBufferIndex < Math.min(offset % CHUNK_SIZE_BYTES, startChunkData.length); ++newBufferIndex){
                        newBuffer[newBufferIndex] = startChunkData[newBufferIndex];
                    }
                }
                // Copy this chunk's fill of the new data
                for(; newBufferIndex < newBufferSize; ++newBufferIndex) {
                    newBuffer[newBufferIndex] = data[bytesWritten++];
                }
                // If this is the last chunk we write to, don't forget to copy the existing data
                // at the end of the chunk, if any
                if (chunkNum == endChunk){
                    // Fill the buffer with the remaining data from end chunk, if any
                    for (; newBufferIndex < endChunkData.length; newBufferIndex++){
                        newBuffer[newBufferIndex] = endChunkData[newBufferIndex];
                    }
                }
                // Write the chunk we just made to the database
                transaction.set(chunkSpace.pack(chunkNum), newBuffer);
            }

            this.incrementVersion(dir, transaction);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Clears the data of this file from the database.
     * Returns false if an error occurs
     */
    public boolean delete(DirectoryLayer directoryLayer, Transaction transaction) {
        try {
            // Load the chunk space to delete all the chunks
            DirectorySubspace chunkSpace = directoryLayer.createOrOpen(transaction, chunksPath).get();
            transaction.clear(chunkSpace.range());
            // Delete the chunk space itself
            directoryLayer.removeIfExists(transaction, chunksPath).get();
            // Load the file space to delete the metadata entries
            DirectorySubspace fileSpace = directoryLayer.createOrOpen(transaction, path).get();
            transaction.clear(fileSpace.range());
            // Delete the file space itself
            directoryLayer.removeIfExists(transaction, path).get();
            // Clear the cache for this file
            FsCacheSingleton.removeFileFromCache(rawPath);
            // Invalidate the cache of the parent dir
            incrementParentVersion(directoryLayer, transaction);;
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public Attr loadMetadata(DirectoryLayer directoryLayer, ReadTransaction readTransaction) {
        Attr attr = new Attr().setObjectType(ObjectType.FILE);

        try {
            DirectorySubspace fileSpace = directoryLayer.open(readTransaction, path).get();
            List<KeyValue> metadata = readTransaction.getRange(fileSpace.range()).asList().get();

            for (KeyValue kv : metadata) {
                String key = fileSpace.unpack(kv.getKey()).getString(0);
                Tuple value = Tuple.fromBytes(kv.getValue());
                switch (key) {
                    case Metadata.TIMESTAMP:
                        attr = attr.setTimestamp(value.getLong(0));
                        System.err.println("Reading timestamp " + attr.getTimestamp());
                        break;
                    case Metadata.MODE:
                        attr = attr.setMode(value.getLong(0));
                        break;
                    case Metadata.GROUP:
                        attr.setGid(value.getLong(0));
                        break;
                    case Metadata.USER:
                        attr.setUid(value.getLong(0));
                    default:
                        break;
                }
            }
        } catch (Exception ignored){}

        return attr;
    }

    public Attr getMetadata(DirectoryLayer directoryLayer, ReadTransaction transaction) {
        return getCache(directoryLayer, transaction).getMetadata();
    }

    /**
     * Set the mode of this file (chmod)
     * Returns false if fails
     */
    public boolean setMode(DirectoryLayer directoryLayer, Transaction transaction, long mode, long userId) {
        if (getCache(directoryLayer, transaction).getMetadata().getUid() != userId) {
            return false;
        }

        try {
            DirectorySubspace filespace = directoryLayer.open(transaction, path).get();
            transaction.set(filespace.pack(Metadata.MODE), Tuple.from(mode).pack());
            this.incrementVersion(directoryLayer, transaction);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Set the timestamp on this file
     * Returns false if fails
     */
    public boolean setTimestamp(DirectoryLayer directoryLayer, Transaction transaction, long unixTimeSeconds) {
        try {
            DirectorySubspace fileSpace = directoryLayer.open(transaction, path).get();
            System.err.println("Setting timestamp to " + unixTimeSeconds);
            transaction.set(fileSpace.pack(Metadata.TIMESTAMP), Tuple.from(unixTimeSeconds).pack());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean truncate(DirectoryLayer directoryLayer, Transaction transaction, long size, long userId) {
        System.out.println("Truncating file to " + size);
        if (!modifyPermitted(directoryLayer, transaction, userId)) {
            return false;
        }

        // Calculate how much we need to delete
        int currentSize = this.size(directoryLayer, transaction);
        int bytesToDelete = currentSize - (int) size;

        // If new size is bigger, ignore
        if (bytesToDelete < 0) {
            return true;
        }

        try {
            // Open chunk space
            DirectorySubspace chunkSpace = directoryLayer.createOrOpen(transaction, chunksPath).get();

            // Calculate which chunks we need to delete
            int lastChunk = currentSize / CHUNK_SIZE_BYTES;
            int newLastChunk = (int) size / CHUNK_SIZE_BYTES;

            if (newLastChunk + 1 < lastChunk) {
                // Clear out data at end of file
                transaction.clear(chunkSpace.pack(newLastChunk + 1), chunkSpace.pack(lastChunk));
            }

            // Check how much data of the new last chunk we need to delete
            currentSize = this.size(directoryLayer, transaction);
            bytesToDelete = currentSize - (int) size;

            // Update the last chunk to have data removed
            if (bytesToDelete > 0) {
                byte[] chunkData = getCache(directoryLayer, transaction).getData(newLastChunk);
                byte[] newChunkData = new byte[chunkData.length - bytesToDelete];
                System.arraycopy(chunkData, 0, newChunkData, 0, newChunkData.length);
                transaction.set(chunkSpace.pack(newLastChunk), newChunkData);
            }
            this.incrementVersion(directoryLayer, transaction);
            System.out.println("Truncate successful");
            return true;

        } catch (Exception e) {
            return false;
        }
    }


    public boolean setOwnership(DirectoryLayer directoryLayer, Transaction tr, long uid, long gid) {
        try {
            DirectorySubspace fileSpace = directoryLayer.open(tr, path).get();
            tr.set(fileSpace.pack(Metadata.USER), Tuple.from(uid).pack());
            tr.set(fileSpace.pack(Metadata.GROUP), Tuple.from(gid).pack());
            incrementVersion(directoryLayer, tr);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public int open(DirectoryLayer directoryLayer, Transaction tr, int flags) {
        return (int) getVersion(directoryLayer, tr);
    }

    private boolean modifyPermitted(DirectoryLayer directoryLayer, ReadTransaction rt, long userId) {
        return checkPermission(directoryLayer, rt, userId, 0200, 0002);
    }

    private boolean readPermitted(DirectoryLayer directoryLayer, ReadTransaction rt, long userId) {
        return checkPermission(directoryLayer, rt, userId, 0400, 0004);
    }

    private boolean checkPermission(DirectoryLayer directoryLayer,
                                    ReadTransaction rt,
                                    long userId,
                                    long userMask,
                                    long otherMask) {
        Attr metadata = getMetadata(directoryLayer, rt);
        return Util.checkPermission(metadata.getMode(), metadata.getUid(), userId, userMask, otherMask);
    }

    public List<byte[]> loadChunks(DirectoryLayer directoryLayer, ReadTransaction rt) {
        try {
            DirectorySubspace chunkSpace = directoryLayer.open(rt, chunksPath).get();

            return rt.getRange(chunkSpace.range())
                    .asList()
                    .get()
                    .stream()
                    .map(KeyValue::getValue)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private FileCacheEntry getCache(DirectoryLayer directoryLayer, ReadTransaction rt) {
        ensureCacheEntryPresent(directoryLayer, rt);

        Optional<FileCacheEntry> cacheEntry = FsCacheSingleton.getFile(rawPath);
        if (!cacheEntry.isPresent()) {
            throw new IllegalStateException("Cache empty even after load! " + rawPath);
        }

        return cacheEntry.get().reloadIfOutdated(directoryLayer, rt);
    }

    private void ensureCacheEntryPresent(DirectoryLayer directoryLayer, ReadTransaction rt) {
        if (!FsCacheSingleton.fileInCache(rawPath)) {
            FsCacheSingleton.loadFileToCache(rawPath, directoryLayer, rt);
        }
    }

    public FileSchema move(DirectoryLayer directoryLayer, Transaction transaction, String newPath) {
        // Create the new file subspace
        FileSchema newNode = new FileSchema(newPath);
        newNode.create(directoryLayer, transaction);

        // Copy the metadata to the new destination
        Attr currentMetadata = this.getMetadata(directoryLayer, transaction);
        newNode.setOwnership(directoryLayer, transaction, currentMetadata.getUid(), currentMetadata.getGid());
        newNode.setMode(directoryLayer, transaction, currentMetadata.getMode(), currentMetadata.getUid());
        newNode.setTimestamp(directoryLayer, transaction, currentMetadata.getTimestamp());

        // Copy the file's bytes to the new file
        byte[] data = this.read(directoryLayer, transaction, 0, this.size(directoryLayer, transaction), currentMetadata.getUid());
        newNode.write(directoryLayer, transaction, data, 0, currentMetadata.getUid());

        // Remove this node
        this.delete(directoryLayer, transaction);

        // Return the new file schema
        return newNode;
    }
}
