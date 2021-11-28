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
            List<String> children,
            DirectorySchema schema,
            DirectoryLayer directoryLayer,
            ReadTransaction rt) {
        DirectoryCacheEntry entry = new DirectoryCacheEntry();
        entry.schema = schema;
        return entry.reload(directoryLayer, rt, children);
    }

    public DirectoryCacheEntry reload(DirectoryLayer directoryLayer, ReadTransaction rt, List<String> children) {
        this.version = schema.getVersion(directoryLayer, rt);
        this.metadata = schema.loadMetadata(directoryLayer, rt);
        this.children = children;

        return this;
    }

    public boolean isCurrent(DirectoryLayer directoryLayer, ReadTransaction rt)  {
        return this.version == schema.getVersion(directoryLayer, rt);
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
