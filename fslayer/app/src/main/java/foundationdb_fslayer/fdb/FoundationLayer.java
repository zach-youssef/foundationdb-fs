package foundationdb_fslayer.fdb;

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import foundationdb_fslayer.cache.FsCacheSingleton;
import foundationdb_fslayer.fdb.object.Attr;
import foundationdb_fslayer.fdb.object.DirectorySchema;
import foundationdb_fslayer.fdb.object.FileSchema;
import foundationdb_fslayer.fdb.object.ObjectType;

import java.util.ArrayList;
import java.util.List;
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
  public byte[] read(String path, long offset, long size, int version) {
    FileSchema file = new FileSchema(path);

    return dbRead(transaction -> file.read(directoryLayer, transaction, offset, size, version));
  }

  @Override
  public boolean rmdir(String path) {
    DirectorySchema dir = new DirectorySchema(path);

    return dbWrite(transaction -> dir.delete(directoryLayer, transaction));
  }

  @Override
  public DirectorySubspace mkdir(String path) {
    DirectorySchema dir = new DirectorySchema(path);

    return dbWrite(transaction -> dir.create(directoryLayer, transaction));
  }


  @Override
  public void write(String path, byte[] data, long offset, int version) {
    FileSchema file = new FileSchema(path);

    dbWrite(transaction -> file.write(directoryLayer, transaction, data, offset, version));
  }


  @Override
  public List<String> ls(String path) {
    try {
      return directoryLayer.list(db, parsePath(path)).get();
    } catch (Exception e) {
      return null;
    }
  }

  public void clearFileContent(String filepath) {
    FileSchema file = new FileSchema(filepath);
    dbWrite(transaction -> file.delete(directoryLayer, transaction));
  }

  @Override
  public boolean createFile(String path) {
    FileSchema file = new FileSchema(path);
    return dbWrite(transaction -> file.create(directoryLayer, transaction));
  }

  @Override
  public Attr getAttr(String path) {
    List<String> paths = parsePath(path);
    List<String> listDotPath = new ArrayList<>(paths);
    listDotPath.add(DirectorySchema.Metadata.META_ROOT);

    return dbRead(rt -> {
      try {
        directoryLayer.open(rt, paths).get();
        if (directoryLayer.exists(rt, listDotPath).get()) {
          return new DirectorySchema(path).getMetadata(directoryLayer, rt);
        } else {
          return new FileSchema(path).getMetadata(directoryLayer, rt);
        }
      } catch (Exception e) {return new Attr().setObjectType(ObjectType.NOT_FOUND); }
    });
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
  public boolean truncate(String path, long size) {
    return dbWrite(tr -> new FileSchema(path).truncate(directoryLayer, tr, size));
  }

  @Override
  // TODO check if file or directory, then set mode accordingly
  public boolean chmod(String path, long mode) {
    return dbWrite(tr -> new FileSchema(path).setMode(directoryLayer, tr, mode));
  }

  @Override
  public boolean chown(String path, long uid, long gid) {
    return dbWrite(tr -> new FileSchema(path).setOwnership(directoryLayer, tr, uid, gid));
  }

  @Override
  public int open(String path, int flags) {
    return dbWrite(tr -> new FileSchema(path).open(directoryLayer, tr, flags));
  }
}
