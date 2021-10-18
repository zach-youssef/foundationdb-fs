package foundationdb_fslayer.fuse;

import foundationdb_fslayer.fdb.FoundationFileOperations;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

public class FuseLayer extends FuseStubFS {
    private final FoundationFileOperations dbOps;

    public FuseLayer(FoundationFileOperations dbOps){
        this.dbOps = dbOps;
    }

    @Override
    public int getattr(String path, FileStat stat) {
        return 0;
    }

    @Override
    public int opendir(String path, FuseFileInfo fi) {
        return 0;
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        return 0;
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, long offset, FuseFileInfo fi) {
        filter.apply(buf, dbOps.HelloWorld(), null, 0);
        return 0;
    }
}
