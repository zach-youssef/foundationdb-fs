package foundationdb_fslayer.fuse;

import com.apple.foundationdb.directory.DirectoryLayer;
import foundationdb_fslayer.fdb.FoundationFileOperations;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;
import ru.serce.jnrfuse.struct.Timespec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static foundationdb_fslayer.Util.parsePath;

public class FuseLayer extends FuseStubFS {

  private final FoundationFileOperations dbOps;
  private final DirectoryLayer dir;

  public FuseLayer(FoundationFileOperations dbOps) {
    this.dbOps = dbOps;
    this.dir = new DirectoryLayer();
  }

  @Override
  public int getattr(String path, FileStat stat) {
    int res = 0;

    if (path.equals("/")) {
      stat.st_mode.set(FileStat.S_IFDIR | 0755);
      stat.st_nlink.set(2);
      return 0;
    }

    List<String> wholePath = parsePath(path);
    String objName = wholePath.get(wholePath.size() - 1);
    List<String> parentPath = new ArrayList<>(wholePath);
    parentPath.remove(objName);

    if (dbOps.ls(dir, wholePath) != null){
      stat.st_mode.set(FileStat.S_IFDIR | 0755);
      stat.st_nlink.set(2);
    } else if (dbOps.ls(dir, parentPath).contains(objName)) {
      stat.st_mode.set(FileStat.S_IFREG | 0777);
      stat.st_size.set(1000);
    } else {
      return -ErrorCodes.ENOENT();
    }

    return res;
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    return 0;
  }

  @Override
  public int opendir(String path, FuseFileInfo fi) {
    return 0;
  }

  @Override
  public int readdir(String path, Pointer buf, FuseFillDir filter, long offset, FuseFileInfo fi) {
    List<String> contents = dbOps.ls(dir, parsePath(path));

    if (contents != null) {
      filter.apply(buf, ".", null, 0);
      filter.apply(buf, "..", null, 0);

      contents.forEach(item -> filter.apply(buf, item, null, 0));
    }

    return 0;
  }

  @Override
  public int mkdir(String path, long mode) {
    return dbOps.mkdir(dir, parsePath(path)) == null ? -ErrorCodes.ENOENT() : 0;
  }

  @Override
  public int rmdir(String path) {
    return dbOps.rmdir(dir, parsePath(path)) ? 0 : -ErrorCodes.ENOENT();
  }

  @Override
  // TODO Completely ignores offset for now
  public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
    byte[] stored = dbOps.read(path);
    if (stored.length > size) {
      byte[] ret = new byte[(int) size];
      for (int i = 0; i < size; ++i)
        ret[i] = stored[i];
      buf.put(0, ret, 0, (int) size);
      return (int) size;
    } else {
      buf.put(0, stored, 0, stored.length);
      return stored.length;
    }
  }

  @Override
  // TODO Completely ignores offset for now
  public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
    byte[] data = new byte[(int) size];
    buf.get(0, data, 0, (int) size);

    dbOps.write(path, data);

    return (int) size;
  }

  @Override
  public int mknod(String path, long mode, long rdev) {
    dbOps.write(path, new byte[1]);
    return 0;
  }

  @Override
  public int utimens(String path, Timespec[] timespec) {
    return 0;
  }
}
