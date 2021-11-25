package foundationdb_fslayer.cache;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import foundationdb_fslayer.fdb.object.Attr;
import foundationdb_fslayer.fdb.object.DirectorySchema;

import java.util.List;

public class DirectoryCacheEntry {
    private long version;
    private DirectorySchema schema;
    private Attr metadata;
    private List<String> children;

    private DirectoryCacheEntry() {}

    public static DirectoryCacheEntry loadFromDB(
            DirectorySchema schema,
            DirectoryLayer directoryLayer,
            ReadTransaction rt) {
        DirectoryCacheEntry entry = new DirectoryCacheEntry();
        entry.schema = schema;
        return entry.reload(directoryLayer, rt);
    }

    public DirectoryCacheEntry reload(DirectoryLayer directoryLayer, ReadTransaction rt) {
        this.version = schema.getVersion(directoryLayer, rt);
        this.metadata = schema.loadMetadata(directoryLayer, rt);
        this.children = schema.loadChildren(directoryLayer, rt);

        return this;
    }

    public DirectoryCacheEntry reloadIfOutdated(DirectoryLayer directoryLayer, ReadTransaction readTransaction) {
        if (schema.getVersion(directoryLayer, readTransaction) != version) {
            return this.reload(directoryLayer, readTransaction);
        }
        return this;
    }

    public long getVersion() {
        return version;
    }

    public Attr getMetadata() {
        return metadata;
    }

    public List<String> getChildren() {
        return children;
    }
}
