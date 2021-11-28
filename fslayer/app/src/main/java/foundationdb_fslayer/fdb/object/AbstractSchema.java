package foundationdb_fslayer.fdb.object;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

abstract public class AbstractSchema {
    protected abstract String getPath();

    protected abstract DirectorySubspace getMetadataSpace(DirectoryLayer directoryLayer, ReadTransaction rt);

    protected abstract String getVersionKey();

    /**
     * Increments the read version of the directory above this file or directory
     * Called on create / delete to update cached list of children
     */
    public void incrementParentVersion(DirectoryLayer dir, Transaction tr) {
        String path = getPath();
        String parentPath = path.substring(0, path.lastIndexOf("/"));

        if (parentPath.equals("")) {
            parentPath = "/";
        }

        System.out.println("ATTEMPTING TO INCREMENT VERSION OF " + parentPath);

        new DirectorySchema(parentPath).incrementVersion(dir, tr);
    }

    public long getVersion(DirectoryLayer directoryLayer, ReadTransaction rt) {
        try {
            return Tuple.fromBytes(rt.get(getMetadataSpace(directoryLayer, rt).pack(getVersionKey())).get())
                    .getLong(0);
        } catch (Exception e) {
            return -1;
        }
    }

    public long incrementVersion(DirectoryLayer directoryLayer, Transaction tr) {
        long currentVersion = getVersion(directoryLayer, tr);
        try {
            tr.set(getMetadataSpace(directoryLayer, tr).pack(getVersionKey()), Tuple.from(currentVersion + 1).pack());
            return currentVersion + 1;
        } catch (Exception e) {
            return -1;
        }
    }
}
