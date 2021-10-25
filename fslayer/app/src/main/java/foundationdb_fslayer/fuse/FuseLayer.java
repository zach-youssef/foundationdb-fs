package foundationdb_fslayer.fuse;

import foundationdb_fslayer.fdb.FoundationFileOperations;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

public class FuseLayer extends FuseStubFS {

  private final FoundationFileOperations dbOps;

  public FuseLayer(FoundationFileOperations dbOps) {
    this.dbOps = dbOps;
  }

  @Override
  public int getattr(String path, FileStat stat) {
    int res = 0;
    if(path.equals("/")){
      stat.st_mode.set(FileStat.S_IFDIR | 0755);
      stat.st_nlink.set(2);
    } else {
      res = -ErrorCodes.ENOENT();
    }
    return res;
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    return 0;
  }

  @Override
  public int readdir(String path, Pointer buf, FuseFillDir filter, long offset, FuseFileInfo fi) {
    filter.apply(buf, ".", null, 0);
    filter.apply(buf, "..", null, 0);
    return 0;
  }
}
