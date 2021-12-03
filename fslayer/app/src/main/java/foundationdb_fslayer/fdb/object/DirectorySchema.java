package foundationdb_fslayer.fdb.object;

import com.apple.foundationdb.KeyValue;
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


    public static class Metadata {
        public final static String META_ROOT = ".";
        public final static String VERSION = "VERSION";
        public final static String UID = "UID";
        public final static String MODE = "MODE";
    }

    public DirectorySchema(String path){
        this.rawPath = path;
        this.paths = Util.parsePath(path);
        this.metadataPath = new ArrayList<>(paths);
        metadataPath.add(Metadata.META_ROOT);
    }

    public Attr loadMetadata(DirectoryLayer directoryLayer, ReadTransaction rt) {
        Attr attr =  new Attr().setObjectType(ObjectType.DIRECTORY);

        try {
            DirectorySubspace metaSpace = getMetadataSpace(directoryLayer, rt);
            List<KeyValue> metadata = rt.getRange(metaSpace.range()).asList().get();

            for (KeyValue kv : metadata) {
                String key = metaSpace.unpack(kv.getKey()).getString(0);
                Tuple value = Tuple.fromBytes(kv.getValue());
                switch (key) {
                    case Metadata.UID:
                        attr = attr.setUid(value.getLong(0));
                        break;
                    case Metadata.MODE:
                        attr = attr.setMode(value.getLong(0));
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception ignored){}

        return attr;
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

    /**
     *  Attempts to delete this directory from the database.
     *  Will return false on failure.
     */
    public boolean delete(DirectoryLayer dir, Transaction transaction) {
        try {
            transaction.clear(getMetadataSpace(dir, transaction).range());
            dir.removeIfExists(transaction, metadataPath).get();
            dir.removeIfExists(transaction, paths).get();
            incrementParentVersion(dir, transaction);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean setMode(DirectoryLayer directoryLayer, Transaction tr, long mode) {
        try {
            DirectorySubspace metaSpace = getMetadataSpace(directoryLayer, tr);
            tr.set(metaSpace.pack(Metadata.MODE), Tuple.from(mode).pack());
            incrementVersion(directoryLayer, tr);
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
    public DirectorySubspace create(DirectoryLayer dir, Transaction transaction, long mode, long userId) {
        try {
            // Create this directory
            DirectorySubspace subspace =  dir.createOrOpen(transaction, paths).get();
            // Initialize the metadata space
            DirectorySubspace metaSpace = initMetadata(dir, transaction);
            // Set the directory's owner
            transaction.set(metaSpace.pack(Metadata.UID), Tuple.from(userId).pack());
            // Set the directory's permissions
            transaction.set(metaSpace.pack(Metadata.MODE), Tuple.from(mode).pack());
            // Invalidate the cache of the parent directory contents
            incrementParentVersion(dir, transaction);
            return subspace;
        } catch (Exception e) {
            return null;
        }
    }

    public DirectorySubspace initMetadata(DirectoryLayer dir, Transaction transaction) {
        try {
            // Create internal metadata subspace
            DirectorySubspace metaSpace = dir.createOrOpen(transaction, metadataPath).get();
            // Initialize the directory's write version
            transaction.set(metaSpace.pack(Metadata.VERSION), Tuple.from(0).pack());
            return metaSpace;
        } catch (Exception e) {
            System.err.println("Failed to initialize metadata for directory " + rawPath);
            System.err.print("Metadata path = ");
            System.err.println(metadataPath);
            e.printStackTrace();
            return null;
        }
    }
}
