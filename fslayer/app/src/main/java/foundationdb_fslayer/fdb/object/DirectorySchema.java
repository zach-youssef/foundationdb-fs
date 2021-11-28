package foundationdb_fslayer.fdb.object;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import foundationdb_fslayer.Util;
import foundationdb_fslayer.cache.DirectoryCacheEntry;
import foundationdb_fslayer.cache.FsCacheSingleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DirectorySchema extends AbstractSchema{
    private final List<String> paths;
    private final List<String> metadataPath;
    private String rawPath;

    public DirectorySchema(String path){
        this.rawPath = path;
        this.paths = Util.parsePath(path);
        this.metadataPath = new ArrayList<>(paths);
        metadataPath.add(Metadata.META_ROOT);
    }

    public Attr loadMetadata(DirectoryLayer directoryLayer, ReadTransaction rt) {
        // TODO
        return new Attr().setObjectType(ObjectType.DIRECTORY);
    }

    @Override
    protected String getPath() {
        return rawPath;
    }

    @Override
    protected DirectorySubspace getMetadataSpace(DirectoryLayer directoryLayer, ReadTransaction rt) {
        try {
            return directoryLayer.open(rt, metadataPath).get();
        } catch (Exception e) {
            throw new IllegalStateException("Ahhhh");
        }
    }

    @Override
    protected String getVersionKey() {
        return Metadata.VERSION;
    }

    public static class Metadata {
        public final static String META_ROOT = ".";
        public final static String VERSION = "VERSION";
    }

    /**
     *  Attempts to delete this directory from the database.
     *  Will return false on failure.
     */
    public boolean delete(DirectoryLayer dir, Transaction transaction) {
        try {
            dir.removeIfExists(transaction, paths).get();
            transaction.clear(getMetadataSpace(dir, transaction).range());
            dir.removeIfExists(transaction, metadataPath).get();
            incrementParentVersion(dir, transaction);
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
    public DirectorySubspace create(DirectoryLayer dir, Transaction transaction) {
        try {
            // Create this directory
            DirectorySubspace subspace =  dir.createOrOpen(transaction, paths).get();
            // Initialize the metadata space
            initMetadata(dir, transaction);
            // Invalidate the cache of the parent directory contents
            incrementParentVersion(dir, transaction);
            return subspace;
        } catch (Exception e) {
            return null;
        }
    }

    public void initMetadata(DirectoryLayer dir, Transaction transaction) {
        try {
            // Create internal metadata subspace
            DirectorySubspace metaSpace = dir.createOrOpen(transaction, metadataPath).get();
            // Initialize the directory's write version
            transaction.set(metaSpace.pack(Metadata.VERSION), Tuple.from(0).pack());
        } catch (Exception e) {
            System.err.println("Failed to initialize metadata for directory " + rawPath);
            System.err.print("Metadata path = ");
            System.err.println(metadataPath);
            e.printStackTrace();
        }
    }
}
