package foundationdb_fslayer.fdb.object;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import foundationdb_fslayer.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DirectorySchema {
    private final List<String> paths;

    public DirectorySchema(String path){
        this.paths = Util.parsePath(path);
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
            return dir.createOrOpen(transaction, paths).get();
        } catch (Exception e) {
            return null;
        }
    }
}
