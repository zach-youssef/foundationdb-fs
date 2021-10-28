package foundationdb_fslayer.fdb.object;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.primitives.Bytes;

import java.util.ArrayList;
import java.util.List;

import static foundationdb_fslayer.Util.parsePath;

public class FileSchema {
    private final List<String> path;
    private final List<String> dirPath; // TODO remove
    private final String filename; // TODO remove

    public FileSchema(String path) {
        this.path = parsePath(path);
        this.filename = this.path.get(this.path.size() - 1);
        this.dirPath = new ArrayList<>(this.path);
        this.dirPath.remove(filename);
    }

    /**
     * Reads the current value of the file.
     * Will return null on error.
     */
    public byte[] read(DirectoryLayer dir, ReadTransaction transaction){
        try {
            DirectorySubspace subspace = dir.open(transaction, dirPath).get();
            return transaction.get(subspace.pack(filename)).get();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Appends the given bytes to the file.
     * Returns false if an error occurs.
     */
    public boolean write(DirectoryLayer dir, Transaction transaction, byte[] data) {
        byte[] buffer = read(dir, transaction);
        byte[] content;
        if (buffer != null) {
            content = Bytes.concat(buffer, data);
        } else {
            content = data;
        }

        try {
            DirectorySubspace subspace = dir.open(transaction, dirPath).get();
            transaction.set(subspace.pack(filename), content);
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
            DirectorySubspace subspace = directoryLayer.open(transaction, dirPath).get();
            transaction.clear(subspace.pack(filename));
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
