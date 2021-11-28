package foundationdb_fslayer.cache;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import foundationdb_fslayer.fdb.object.DirectorySchema;
import foundationdb_fslayer.fdb.object.FileSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class FsCacheSingleton {
    private static final HashMap<String, FileCacheEntry> FILE_CACHE = new HashMap<>();
    private static final HashMap<String, DirectoryCacheEntry> DIR_CACHE = new HashMap<>();

    public static void loadFileToCache(String path, DirectoryLayer directoryLayer, ReadTransaction rt) {
        FileSchema schema = new FileSchema(path);
        FILE_CACHE.put(path, FileCacheEntry.loadFromDB(schema, directoryLayer, rt));
    }

    public static void removeFileFromCache(String path) {
        FILE_CACHE.remove(path);
    }

    public static Optional<FileCacheEntry> getFile(String path) {
        return Optional.ofNullable(FILE_CACHE.getOrDefault(path, null));
    }

    public static boolean fileInCache(String path) {
        return FILE_CACHE.containsKey(path);
    }

    public static DirectoryCacheEntry loadDirToCache(String path, DirectoryLayer directoryLayer, ReadTransaction rt, List<String> children) {
        DirectorySchema schema = new DirectorySchema(path);
        DIR_CACHE.put(path, DirectoryCacheEntry.loadFromDB(children, schema, directoryLayer, rt));
        return DIR_CACHE.get(path);
    }

    public static void removeDirFromCache(String path) {
        DIR_CACHE.remove(path);
    }

    public static boolean dirInCache(String path) {
        return DIR_CACHE.containsKey(path);
    }

    public static Optional<DirectoryCacheEntry> getDir(String path) {
        return Optional.ofNullable(DIR_CACHE.getOrDefault(path, null));
    }
}
