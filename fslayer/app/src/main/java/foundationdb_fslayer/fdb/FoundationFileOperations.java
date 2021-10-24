package foundationdb_fslayer.fdb;

import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectorySubspace;
import java.util.List;

public interface FoundationFileOperations {

    String HelloWorld();
    void write(String  path, String data);




  /**
   * Simple Hello World program to start with.
   *
   * @return a string of "hello world"
   */
  String helloWorld();

  /**
   * Read a file.
   *
   * @param path file path
   * @return encoded byte representation of the file content
   */
  byte[] read(String path);

  /**
   * Remove an empty directory if exists.
   *
   * @param dir   parent directory
   * @param paths list of path strings
   * @return a boolean value determining whether removing the directory is successful
   */
  boolean rmdir(Directory dir, List<String> paths);

  /**
   * Create a new directory matching the provided path under the given directory.
   *
   * @param dir   parent directory
   * @param paths list of path strings
   * @return The directory subspace of the newly created directory
   */
  DirectorySubspace mkdir(Directory dir, List<String> paths);

  /**
   * List all directories under the provided directory.
   *
   * @param dir   parent directory
   * @param paths list of path strings
   * @return a list of strings representing all sub-directories under the current directory
   */
  List<String> ls(Directory dir, List<String> paths);

}
