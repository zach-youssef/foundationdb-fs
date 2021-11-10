package foundationdb_fslayer.fuse;

import foundationdb_fslayer.fdb.FoundationFileOperations;
import foundationdb_fslayer.fdb.object.Attr;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Timespec;

import java.util.List;


public class FuseLayer extends FuseStubFS {

  private final FoundationFileOperations dbOps;

  public FuseLayer(FoundationFileOperations dbOps) {
    this.dbOps = dbOps;
  }

  @Override
  public int getattr(String path, FileStat stat) {
    int res = 0;

    if (path.equals("/")) {
      stat.st_mode.set(FileStat.S_IFDIR | 0755);
      stat.st_nlink.set(2);
      return 0;
    }

    Attr attr = dbOps.getAttr(path);

    switch (attr.getObjectType()) {
      case FILE:
        stat.st_mode.set(FileStat.S_IFREG | 0777);
        stat.st_size.set(dbOps.getFileSize(path));
        break;
      case DIRECTORY:
        stat.st_mode.set(FileStat.S_IFDIR | 0755);
        stat.st_nlink.set(2);
        break;
      case NOT_FOUND:
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
    List<String> contents = dbOps.ls(path);

    if (contents != null) {
      filter.apply(buf, ".", null, 0);
      filter.apply(buf, "..", null, 0);

      contents.forEach(item -> filter.apply(buf, item, null, 0));
    }

    return 0;
  }

  @Override
  public int mkdir(String path, long mode) {
    return dbOps.mkdir(path) == null ? -ErrorCodes.ENOENT() : 0;
  }

  @Override
  public int rmdir(String path) {
    return dbOps.rmdir(path) ? 0 : -ErrorCodes.ENOENT();
  }

  @Override
  public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
    byte[] stored = dbOps.read(path, offset, size);
    buf.put(0, stored, 0, stored.length);
    return stored.length;
  }

  @Override
  public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
    byte[] data = new byte[(int) size];
    buf.get(0, data, 0, (int) size);

    dbOps.write(path, data, offset);

    return (int) size;
  }

  @Override
  public int mknod(String path, long mode, long rdev) {
    return dbOps.createFile(path) ? 0 : -ErrorCodes.ENOENT();
  }

  @Override
  public int utimens(String path, Timespec[] timespec) {
    return dbOps.setFileTime(timespec[0].tv_sec.get(), path) ? 0 : -ErrorCodes.ENOENT();
  }

  @Override
  public int unlink(String path){
    dbOps.clearFileContent(path);
    return 0;
  }

  @Override
  public int truncate(String path, long size) {
    return dbOps.truncate(path,size) ? 0 : -ErrorCodes.ENOENT();
  }
}
