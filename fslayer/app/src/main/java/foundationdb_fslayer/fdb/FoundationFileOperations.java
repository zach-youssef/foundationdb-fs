package foundationdb_fslayer.fdb;

import com.apple.foundationdb.directory.DirectorySubspace;
import java.util.List;

public interface FoundationFileOperations {
  /**
   * Read a file.
   *
   * @param path file path
   * @return encoded byte representation of the file content
   */
  byte[] read(String path);

  /**
   * Write to a file
   *
   * @param path file path
   * @param data data to be added to file
   */
  void write(String path, byte[] data);

  /**
   * Remove an empty directory if exists.
   *
   * @param path list of path strings
   * @return a boolean value determining whether removing the directory is successful
   */
  boolean rmdir(String path);

  /**
   * Create a new directory matching the provided path under the given directory.
   *
   * @param path list of path strings
   * @return The directory subspace of the newly created directory
   */
  DirectorySubspace mkdir(String path);

  /**
   * List all directories under the provided directory.
   *
   * @param path list of path strings
   * @return a list of strings representing all sub-directories under the current directory
   */
  List<String> ls(String path);

  /**
   * Clear content of file
   *
   * @param file file path
   */
  void clearFileContent(String file);

}
