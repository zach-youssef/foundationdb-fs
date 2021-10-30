package foundationdb_fslayer.fdb.object;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

import static foundationdb_fslayer.Util.parsePath;

public class FileSchema {
    private final List<String> path;
    private final List<String> chunksPath;

    public final static int CHUNK_SIZE_BYTES = 1000;

    private static class Metadata {
        final static String CHUNKS = "CHUNKS";
        final static String TIMESTAMP = "TIMESTAMP";
        final static String SIZE = "SIZE";
    }

    public FileSchema(String path) {
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

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Reads the current value of the file.
     * Will return null on error.
     */
    public byte[] read(DirectoryLayer dir, ReadTransaction transaction){
        try {
            // Open the file's chunk space
            DirectorySubspace chunkSpace = dir.open(transaction, chunksPath).get();

            // Grab all the file's data
            List<KeyValue> chunks = transaction.getRange(chunkSpace.range()).asList().get();


            // Initialize buffer to store file
            byte[] data = new byte[this.size(dir, transaction)];

            // Grab all the chunks from the database and copy them into the buffer
            int index = 0;
            for (KeyValue kv: chunks) {
                for (byte b : kv.getValue()) {
                    data[index++] = b;
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
            DirectorySubspace chunkSpace = dir.open(transaction, chunksPath).get();
            List<KeyValue> chunks = transaction.getRange(chunkSpace.range()).asList().get();
            return ((chunks.size() - 1) * CHUNK_SIZE_BYTES) + (chunks.get(chunks.size() - 1).getValue().length);
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Appends the given bytes to the file.
     * Returns false if an error occurs.
     */
    public boolean write(DirectoryLayer dir, Transaction transaction, byte[] data, long offset) {
        try {
            DirectorySubspace chunkSpace = dir.open(transaction, chunksPath).get();

            // Grab existing chunks we will be writing to
            int startChunk = (int) (offset / CHUNK_SIZE_BYTES);
            int endChunk = (int) ((offset + data.length) / CHUNK_SIZE_BYTES);
            // Grab the chunks we are not completely overwriting
            byte[] startChunkData = transaction.get(chunkSpace.pack(startChunk)).get();
            byte[] endChunkData = transaction.get(chunkSpace.pack(endChunk)).get();

            if (startChunkData == null) startChunkData = new byte[0];
            if (endChunkData == null) endChunkData = new byte[0];

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
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public Attr getMetadata(DirectoryLayer directoryLayer, ReadTransaction transaction) {
        Attr attr = new Attr().setObjectType(ObjectType.FILE);

        try {
            DirectorySubspace fileSpace = directoryLayer.open(transaction, path).get();
            List<KeyValue> metadata = transaction.getRange(fileSpace.range()).asList().get();

            for (KeyValue kv : metadata) {
                String key = fileSpace.unpack(kv.getKey()).getString(0);
                Tuple value = Tuple.fromBytes(kv.getValue());
                switch (key) {
                    case Metadata.TIMESTAMP:
                        attr = attr.setTimestamp(value.getLong(0));
                    default:
                        break;
                }
            }
        } catch (Exception ignored){}

        return attr;
    }

    /**
     * Set the timestamp on this file
     * Returns false if fails
     */
    public boolean setTimestamp(DirectoryLayer directoryLayer, Transaction transaction, long unixTimeSeconds) {
        try {
            DirectorySubspace fileSpace = directoryLayer.open(transaction, path).get();
            transaction.set(fileSpace.pack(Metadata.TIMESTAMP), Tuple.from(unixTimeSeconds).pack());
            return true;
        } catch (Exception e) {
            return false;
        }
    }


}
