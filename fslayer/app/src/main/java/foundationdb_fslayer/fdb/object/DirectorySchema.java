package foundationdb_fslayer.fdb.object;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import foundationdb_fslayer.Util;

import java.util.ArrayList;
import java.util.List;

public class DirectorySchema {
    private final List<String> paths;
    private final List<String> metadataPath;

    public DirectorySchema(String path){
        this.paths = Util.parsePath(path);
        this.metadataPath = new ArrayList<>(paths);
        paths.add(Metadata.META_ROOT);
    }

    private static class Metadata {
        final static String META_ROOT = ".";
    }

    /**
     *  Returns the list of file and directory names contained within this directory.
     *  Will return null if this directory does not exist in the database.
     */
    public List<String> list(Directory dir, ReadTransaction transaction) {
        try {
            return dir.list(transaction, paths).get();
        } catch (Exception e){
            return null;
        }
    }

    /**
     *  Attempts to delete this directory from the database.
     *  Will return false on failure.
     */
    public boolean delete(Directory dir, Transaction transaction) {
        try {
            dir.removeIfExists(transaction, paths).get();
            dir.removeIfExists(transaction, metadataPath).get();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Creates this directory in the database.
     * Will return the subspace created, or null if failure occurs.
     * Will silently succeed if the directory already exists.
     */
    public DirectorySubspace create(Directory dir, Transaction transaction) {
        try {
            // Create this directory
            DirectorySubspace subspace =  dir.createOrOpen(transaction, paths).get();
            // Create internal metadata subspace
            dir.createOrOpen(transaction, metadataPath).get();
            return subspace;
        } catch (Exception e) {
            return null;
        }
    }
}
