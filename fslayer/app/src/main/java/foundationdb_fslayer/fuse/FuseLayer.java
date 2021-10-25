package foundationdb_fslayer.fuse;

import com.apple.foundationdb.directory.DirectoryLayer;
import foundationdb_fslayer.fdb.FoundationFileOperations;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

    List<String> paths = Arrays.stream(path.split("/"))
            .filter(str -> !str.equals(""))
            .collect(Collectors.toList());

    if (dbOps.ls(dir, paths) != null){
      stat.st_mode.set(FileStat.S_IFDIR | 0755);
      stat.st_nlink.set(2);
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
    List<String> contents = dbOps.ls(dir, Arrays.asList(path.split("/")));

    if (contents != null) {
      filter.apply(buf, ".", null, 0);
      filter.apply(buf, "..", null, 0);

      contents.forEach(item -> filter.apply(buf, item, null, 0));
    }

    return 0;
  }
}
