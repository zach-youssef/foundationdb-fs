package foundationdb_fslayer.cache;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import foundationdb_fslayer.fdb.object.Attr;
import foundationdb_fslayer.fdb.object.FileSchema;

import java.util.ArrayList;
import java.util.List;

public class FileCacheEntry {
    private List<byte[]> data;
    private Attr metadata;
    private long version;
    private FileSchema schema;

    private FileCacheEntry() {}

    public static FileCacheEntry loadFromDB(
            FileSchema schema,
            DirectoryLayer directoryLayer,
            ReadTransaction rt) {
        FileCacheEntry entry = new FileCacheEntry();
        entry.schema = schema;
        return entry.reload(directoryLayer, rt);
    }

    public boolean isCurrent(DirectoryLayer directoryLayer, ReadTransaction rt) {
        return this.version == this.schema.getVersion(directoryLayer, rt);
    }

    public FileCacheEntry reload(DirectoryLayer directoryLayer, ReadTransaction rt){
        this.version = schema.getVersion(directoryLayer, rt);
        this.metadata = schema.loadMetadata(directoryLayer, rt);
        this.data = schema.loadChunks(directoryLayer, rt);
        return this;
    }

    public FileCacheEntry reloadIfOutdated(DirectoryLayer directoryLayer, ReadTransaction rt) {
        if (isCurrent(directoryLayer, rt)) {
            return this;
        }
        return this.reload(directoryLayer, rt);
    }

    public byte[] getData(int chunkIndex) {
        if (chunkIndex < data.size()) {
            return data.get(chunkIndex);
        } else {
            return new byte[0];
        }
    }

    public List<byte[]> getData(int startIndex, int endIndex) {
        List<byte[]> splice = new ArrayList<>();
        for (int i = startIndex; i <= endIndex && i < data.size(); ++i) {
            splice.add(data.get(i));
        }
        return splice;
    }

    public List<byte[]> getData() {
        return this.data;
    }

    public Attr getMetadata() {
        return metadata;
    }
}
