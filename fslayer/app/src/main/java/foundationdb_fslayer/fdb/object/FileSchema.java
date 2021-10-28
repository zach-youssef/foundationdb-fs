package foundationdb_fslayer.fdb.object;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static foundationdb_fslayer.Util.parsePath;

public class FileSchema {
    private final List<String> path;
    private final List<String> chunksPath;

    private static class Metadata {
        final static String CHUNKS = "CHUNKS";
        final static String CHUNK_INFO = "CHUNKINFO";
        final static String TIMESTAMP = "TIMESTAMP";
        final static String MODE = "MODE";
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
            // Initialize chunkInfo file
            transaction.set(fileSpace.pack(Metadata.CHUNK_INFO), Tuple.fromList(Arrays.asList(0L)).pack());
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
            // Open the file's file & chunk space
            DirectorySubspace fileSpace = dir.open(transaction, path).get();
            // Calculate total size of the file
            Tuple chunkInfo = Tuple.fromBytes(transaction.get(fileSpace.pack(Metadata.CHUNK_INFO)).get());
            int totalLength = 0;
            for (int i = 0; i < chunkInfo.size(); ++i){
                totalLength += chunkInfo.getLong(i);
            }
            // Initialize buffer to store file
            byte[] data = new byte[totalLength];
            // Grab all the chunks from the database and copy them into the buffer
            DirectorySubspace chunkSpace = dir.open(transaction, chunksPath).get();
            int index = 0;
            for (KeyValue kv: transaction.getRange(chunkSpace.range()).asList().get()) {
                for (byte b : kv.getValue()) {
                    data[index++] = b;
                }
            }
            return data;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Appends the given bytes to the file.
     * Returns false if an error occurs.
     */
    public boolean write(DirectoryLayer dir, Transaction transaction, byte[] data) {
        try {
            // Check chunk info to know the new chunk to add
            DirectorySubspace fileSpace = dir.open(transaction, path).get();
            Tuple chunkInfo = Tuple.fromBytes(transaction.get(fileSpace.pack(Metadata.CHUNK_INFO)).get());
            int newChunkIndex = chunkInfo.size();

            // Write the new chunk
            DirectorySubspace chunkSpace = dir.open(transaction, chunksPath).get();
            transaction.set(chunkSpace.pack(newChunkIndex), data);

            // Update the chunk info
            List<Long> newChunkInfo = new ArrayList<>();
            for (int i = 0; i < chunkInfo.size(); ++i){
                newChunkInfo.add(chunkInfo.getLong(i));
            }
            newChunkInfo.add((long) data.length);
            transaction.set(fileSpace.pack(Metadata.CHUNK_INFO), Tuple.fromList(newChunkInfo).pack());

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
            DirectorySubspace chunkSpace = directoryLayer.open(transaction, chunksPath).get();
            transaction.clear(chunkSpace.range());
            // Delete the chunk space itself
            chunkSpace.remove(transaction);
            // Load the file space to delete the metadata entries
            DirectorySubspace fileSpace = directoryLayer.open(transaction, path).get();
            transaction.clear(fileSpace.range());
            // Delete the file space itself
            fileSpace.remove(transaction);
            return true;
        } catch (Exception e) {
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
