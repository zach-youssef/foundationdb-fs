package foundationdb_fslayer.cache;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import foundationdb_fslayer.fdb.object.FileSchema;

import java.util.HashMap;
import java.util.Optional;

public class FsCacheSingleton {
    private static final HashMap<String, FileCacheEntry> FILE_CACHE = new HashMap<>();

    public static void loadToCache(String path, DirectoryLayer directoryLayer, ReadTransaction rt) {
        FileSchema schema = new FileSchema(path);
        FILE_CACHE.put(path, FileCacheEntry.loadFromDB(schema, directoryLayer, rt));
    }

    public static void removeFromCache(String path) {
        FILE_CACHE.remove(path);
    }

    public static Optional<FileCacheEntry> getFile(String path) {
        return Optional.ofNullable(FILE_CACHE.getOrDefault(path, null));
    }

    public static boolean inCache(String path) {
        return FILE_CACHE.containsKey(path);
    }
}
