package foundationdb_fslayer.fdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.primitives.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static foundationdb_fslayer.Util.parsePath;

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
    List<String> paths = parsePath(path);
    String filename = paths.get(paths.size() - 1);
    List<String> dirPath = new ArrayList<>(paths);
    dirPath.remove(filename);
    try {
      return db.read(rt -> {
        try {
          DirectorySubspace subspace = new DirectoryLayer().open(rt, dirPath).get();
          return rt.get(subspace.pack(filename));
        } catch (Exception e){
          System.err.println("read error");
          e.printStackTrace();
          return null;
        }
      }).get();
    } catch (Exception e){
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
  public void write(String path, byte[] data) {
    List<String> paths = parsePath(path);
    String filename = paths.get(paths.size() - 1);
    List<String> dirPath = new ArrayList<>(paths);
    dirPath.remove(filename);

    // Read existing value of path from the database
    byte[] buffer = read(path);

    db.run(tr -> {
      byte[] content;
      if (buffer != null) {
        // Add data to existing buffer
        content = Bytes.concat(buffer, data);
      } else {
        content = data;
      }

      try {
        DirectorySubspace subspace = new DirectoryLayer().open(tr, dirPath).get();
        tr.set(subspace.pack(filename), content);
      }catch(Exception e) {
        System.err.println("Error writing");
        e.printStackTrace();
      }
      return null;
    });
  }


  @Override
  public List<String> ls(Directory dir, List<String> paths) {
    try {
      final List<String> contents = new ArrayList<>(dir.list(db, paths).get());

      final List<String> filenames = db.read(readTransaction -> {
        try {
          DirectorySubspace subspace = dir.open(db, paths).get();
          Range keyRange = subspace.range();
          return readTransaction.getRange(keyRange).asList().get().stream()
                  .map(kv -> Tuple.fromBytes(kv.getKey()))
                  .map(t -> t.getString(t.size() - 1))
                  .collect(Collectors.toList());
        } catch (Exception e) {
          e.printStackTrace();
          return null;
        }
      });

      if (filenames != null) {
        contents.addAll(filenames);
      }

      return contents;
    } catch (Exception e) {
      return null;
    }
  }

  public void clearFileContent(String file) {
    List<String> paths = parsePath(file);
    String filename = paths.get(paths.size() - 1);
    List<String> dirPath = new ArrayList<>(paths);
    dirPath.remove(filename);

    db.run(tr -> {
      try {
        DirectorySubspace subspace = new DirectoryLayer().open(tr, dirPath).get();
        tr.clear(subspace.pack(filename));
      } catch (Exception e) {e.printStackTrace();}
      return null;
    });
  }
}
