package foundationdb_fslayer.fdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.primitives.Bytes;
import java.util.List;

public class FoundationLayer implements FoundationFileOperations {

  private final FDB fdb;
  private final Database db;

  public FoundationLayer(Integer apiVersion) {
    this.fdb = FDB.selectAPIVersion(apiVersion);
    this.db = fdb.open();
  }

  @Override
  public String helloWorld() {
    try {
      String hello;
      // Run an operation on the database
      db.run(tr -> {
        tr.set(Tuple.from("hello").pack(), Tuple.from("world").pack());
        return null;
      });
      // Get the value of 'hello' from the database
      hello = db.run(tr -> {
        byte[] result = tr.get(Tuple.from("hello").pack()).join();
        return Tuple.fromBytes(result).getString(0);
      });
      return hello;
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public byte[] read(String path) {
    return db.read(tr -> tr.get(Tuple.from(path).pack()).join());
  }

  @Override
  public boolean rmdir(Directory dir, List<String> paths) {
    try {
      dir.removeIfExists(db, paths).get();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public DirectorySubspace mkdir(Directory dir, List<String> paths) {
    try {
      return dir.create(db, paths).get();
    } catch (Exception e) {
      return null;
    }
  }


  @Override
  public void write(String path, byte[] data) {

    // Read existing value of path from the database
    byte[] buffer = read(path);

    // Add data to existing buffer,
    if(buffer != null) {
      db.run(tr -> {
        byte[] content = Bytes.concat(buffer, data);
        tr.set(Tuple.from(path).pack(),content);
        return null;
      });
    } else {
      db.run(tr -> {
        tr.set(Tuple.from(path).pack(), data);
        return null;
      });
    }
  }


  @Override
  public List<String> ls(Directory dir, List<String> paths) {
    try {
      return dir.list(db, paths).get();
    } catch (Exception e) {
      return null;
    }
  }

  public void clearFileContent(String file) {
    db.run(tr -> {
      tr.clear(Tuple.from(file).pack());
      return null;
    });

  }

}
