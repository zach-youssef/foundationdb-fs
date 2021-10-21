package foundationdb_fslayer.fdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import java.util.List;

public class FoundationLayer implements FoundationFileOperations {

  private final FDB fdb;
  private final Database db;

  public FoundationLayer(FDB fdb) {
    this.fdb = fdb;
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
  public List<String> ls(Directory dir, List<String> paths) {
    try {
      return dir.list(db, paths).get();
    } catch (Exception e) {
      return null;
    }
  }
}
