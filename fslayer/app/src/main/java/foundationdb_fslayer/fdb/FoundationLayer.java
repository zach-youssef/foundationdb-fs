package foundationdb_fslayer.fdb;

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import foundationdb_fslayer.Util;
import foundationdb_fslayer.cache.DirectoryCacheEntry;
import foundationdb_fslayer.cache.FsCacheSingleton;
import foundationdb_fslayer.fdb.object.Attr;
import foundationdb_fslayer.fdb.object.DirectorySchema;
import foundationdb_fslayer.fdb.object.FileSchema;
import foundationdb_fslayer.fdb.object.ObjectType;
import foundationdb_fslayer.permissions.PermissionManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static foundationdb_fslayer.Util.parsePath;

public class FoundationLayer implements FoundationFileOperations {

  private final FDB fdb;
  private final DirectoryLayer directoryLayer;
  private final Database db;

  public FoundationLayer(Integer apiVersion) {
    this.fdb = FDB.selectAPIVersion(apiVersion);
    this.directoryLayer = new DirectoryLayer();
    this.db = fdb.open();
  }

  private <T> T dbRead(Function<ReadTransaction, T> op){
    return db.read(op);
  }

  private <T> T dbWrite(Function<Transaction, T> op){
    return db.run(op);
  }

  @Override
  public byte[] read(String path, long offset, long size, long userId) {
    FileSchema file = new FileSchema(path);

    return dbRead(transaction -> file.read(directoryLayer, transaction, offset, size, userId));
  }

  @Override
  public boolean rmdir(String path, long uid) {
    DirectorySchema dir = new DirectorySchema(path);

    return dbWrite(transaction ->
            canNodeBeCreatedOrRemoved(transaction, path, uid)
                    && dir.delete(directoryLayer, transaction));
  }

  @Override
  public DirectorySubspace mkdir(String path, long mode, long uid) {
    DirectorySchema dir = new DirectorySchema(path);

    return dbWrite(transaction -> {
      if (!canNodeBeCreatedOrRemoved(transaction, path, uid)) {
        return null;
      }
      return dir.create(directoryLayer, transaction, mode, uid);
    });
  }


  @Override
  public boolean write(String path, byte[] data, long offset, long userId) {
    FileSchema file = new FileSchema(path);

    return dbWrite(transaction -> file.write(directoryLayer, transaction, data, offset, userId));
  }


  @Override
  public List<String> ls(String path, long userId) {
    // Check if the user is allowed to read this directory
    if (!path.equals("/")
          && !dbRead(tr->checkDirectoryPermission(path, tr, userId, 0400, 0004))) {
      return null;
    }

    Optional<List<String>> cacheValue = dbRead(rt ->
            FsCacheSingleton.getDir(path).flatMap(entry ->
                    entry.isCurrent(directoryLayer, rt)
                            ? Optional.of(entry.getChildren())
                            : Optional.empty()));
    return cacheValue.orElseGet(() -> {
      try {
        List<String> children = directoryLayer.list(db, parsePath(path)).get();
        if (!FsCacheSingleton.dirInCache(path)) {
          dbRead(rt -> {
            FsCacheSingleton.loadDirToCache(path, directoryLayer, rt, children);
            return null;
          });
        } else {
          DirectoryCacheEntry entry = FsCacheSingleton.getDir(path)
                  .orElseThrow(() -> new IllegalStateException("Dir not present after check"));
          dbRead(rt -> entry.reload(directoryLayer, rt, children));
        }
        return children;
      } catch (Exception e) {
        return null;
      }
    });
  }

  private boolean checkDirectoryPermission(String dirPath, ReadTransaction rt, long userId, long userMask, long otherMask) {
    Attr attr = getDirectoryMetadata(dirPath, rt);
    return Util.checkPermission(attr.getMode(), attr.getUid(), userId, userMask, otherMask);
  }

  public boolean clearFileContent(String filepath, long userId) {
    FileSchema file = new FileSchema(filepath);
    return dbWrite(transaction ->
            canNodeBeCreatedOrRemoved(transaction, filepath, userId)
                    && file.delete(directoryLayer, transaction));
  }

  @Override
  public boolean createFile(String path, long userId) {
    FileSchema file = new FileSchema(path);
    return dbWrite(transaction ->
            canNodeBeCreatedOrRemoved(transaction, path, userId)
                    && file.create(directoryLayer, transaction));
  }

  @Override
  public Attr getAttr(String path) {
    return dbRead(rt -> {
      Optional<Boolean> isDir = isDirectory(path, rt);
      if (isDir.isPresent()) {
        if (isDir.get()) {
          return getDirectoryMetadata(path, rt);
        } else {
          return new FileSchema(path).getMetadata(directoryLayer, rt);
        }
      } else {
        return new Attr().setObjectType(ObjectType.NOT_FOUND);
      }
    });
  }

  private Optional<Boolean> isDirectory(String path, ReadTransaction rt) {
    List<String> paths = parsePath(path);
    List<String> listDotPath = new ArrayList<>(paths);
    listDotPath.add(DirectorySchema.Metadata.META_ROOT);
    try {
      directoryLayer.open(rt, paths).get();
      return Optional.of(directoryLayer.exists(rt, listDotPath).get());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private Optional<List<String>> loadDirectoryContents(String path) {
    try {
      return Optional.of(directoryLayer.list(db, parsePath(path)).get());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private Attr getDirectoryMetadata(String path, ReadTransaction rt) {
    DirectoryCacheEntry entry = FsCacheSingleton.getDir(path)
            .flatMap(storedEntry -> {
              if (storedEntry.isCurrent(directoryLayer, rt)) {
                return Optional.of(storedEntry);
              } else {
                return loadDirectoryContents(path)
                        .map(children -> storedEntry.reload(directoryLayer, rt, children));
              }
            })
            .orElseGet(() -> {
              try {
                return FsCacheSingleton.loadDirToCache(
                        path,
                        directoryLayer,
                        rt,
                        directoryLayer.list(rt, parsePath(path)).get());
              } catch (Exception e) {
                return null;
              }
            });

    return entry != null
            ? entry.getMetadata()
            : new Attr().setObjectType(ObjectType.NOT_FOUND);
  }

  @Override
  public boolean setFileTime(Long timestamp, String path) {
    return dbWrite(tr ->
            new FileSchema(path).setTimestamp(directoryLayer, tr, timestamp));
  }

  @Override
  public int getFileSize(String path) {
    return dbRead(rt -> new FileSchema(path).size(directoryLayer, rt));
  }

  @Override
  public boolean truncate(String path, long size, long userId) {
    return dbWrite(tr -> new FileSchema(path).truncate(directoryLayer, tr, size, userId));
  }

  @Override
  public boolean chmod(String path, long mode, long userId) {
    return dbWrite(tr -> isDirectory(path, tr).map(isDir -> {
      if (isDir) {
        return getDirectoryMetadata(path, tr).getUid() == userId
                && new DirectorySchema(path).setMode(directoryLayer, tr, mode);
      } else {
        return new FileSchema(path).setMode(directoryLayer, tr, mode, userId);
      }
    }).orElse(false));
  }

  @Override
  public boolean chown(String path, long uid, long gid) {
    return dbWrite(tr -> new FileSchema(path).setOwnership(directoryLayer, tr, uid, gid));
  }

  @Override
  public int open(String path, int flags) {
    return dbWrite(tr -> new FileSchema(path).open(directoryLayer, tr, flags));
  }

  @Override
  public boolean move(String oldPath, String newPath, long userId) {
    return dbWrite(tr-> {
      String path = newPath;
      if (isDirectory(newPath, tr).orElse(false)) {
        path += oldPath.substring(oldPath.lastIndexOf("/") + 1);
      }
      return recursiveMove(oldPath, path, tr, userId)
              && new DirectorySchema(oldPath).delete(directoryLayer, tr);
    });
  }

  public boolean recursiveMove(String oldPath, String newPath, Transaction transaction, long userId) {
    System.out.printf("Renaming %s to %s\n", oldPath, newPath);
    return isDirectory(oldPath, transaction).map(isDir -> {
      if (isDir) {
        // Grab the metadata from the old directory
        Attr oldMetadata = getDirectoryMetadata(oldPath, transaction);

        // Create the new directory
        DirectorySchema newDir = new DirectorySchema(newPath);
        newDir.create(directoryLayer, transaction, oldMetadata.getMode(), oldMetadata.getUid());

        // Recur on all subdirectories, then on child files
        List<String> childPaths = ls(oldPath, oldMetadata.getUid());
        boolean subDirCopySuccess = childPaths.stream()
                .filter(child -> isDirectory(oldPath + "/" + child, transaction).orElse(false))
                .allMatch(subDir -> recursiveMove(
                        oldPath + "/" + subDir,
                        newPath + "/" + subDir,
                        transaction, userId));

         return subDirCopySuccess &&
                childPaths.stream()
                        .filter(child -> !isDirectory(oldPath + "/" + child, transaction).orElse(true))
                        .filter(child -> !child.equals("."))
                        .allMatch(file -> new FileSchema(oldPath + "/" + file)
                                .move(directoryLayer, transaction, newPath + "/" + file) != null);


      } else {
        new FileSchema(oldPath).move(directoryLayer, transaction, newPath);
        return true;
      }
    }).orElse(false);
  }

  @Override
  public void initRootIfNeeded() {
    db.run(tr -> {
      try {
        if (!directoryLayer.exists(tr, Arrays.asList(DirectorySchema.Metadata.META_ROOT)).get()) {
          new DirectorySchema("/").initMetadata(directoryLayer, tr);
          System.out.println("Root directory created!");
        } else {
          System.out.println("Root directory exists!");
        }
      } catch (Exception e) {
        System.err.println("Error checking if meta root exists to initialize");
        e.printStackTrace();
      }
      return null;
    });
  }

  @Override
  public Optional<PermissionManager> login(String username, String password) {
    return dbWrite(tr -> PermissionManager.login(username, password, directoryLayer, tr));
  }

  private boolean canNodeBeCreatedOrRemoved(ReadTransaction rt, String targetPath, long userId) {
    // A node can be created or removed if the user has permissions to write to the parent directory

    // So, let's first grab the parent directory.
    String parentPath = targetPath.substring(0, targetPath.lastIndexOf("/"));

    // Everyone has access to the root directory
    if (parentPath.equals("")) {
      return true;
    }

    // Return true if the user can both read and write to that directory
    return checkDirectoryPermission(parentPath, rt, userId, 0200, 0002)
            && checkDirectoryPermission(parentPath, rt, userId, 0400, 0004);
  }
}
