package foundationdb_fslayer.fdb;

import com.apple.foundationdb.directory.DirectorySubspace;
import foundationdb_fslayer.fdb.object.Attr;

import java.util.List;

public interface FoundationFileOperations {
  /**
   * Read a file.
   *
   * @param path file path
   * @return encoded byte representation of the file content
   */
  byte[] read(String path, long offset, long size, int version);

  default byte[] read(String path, int version){
    return read(path, 0, getFileSize(path), version);
  }

  /**
   * Write to a file
   *
   * @param path file path
   * @param data data to be added to file
   */
  void write(String path, byte[] data, long offset, int version);

  default void write(String path, byte[] data, int version){
    write(path, data, 0, version);
  }

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

  /**
   * Creates a new empty file at the given path
   * returns false on failure
   */
  boolean createFile(String path);


  /**
   * Get the attributes of this file or directory
   */
  Attr getAttr(String path);

  /**
   * Set the time on a file
   */
  boolean setFileTime(Long timestamp, String path);

  /**
   * Returns a file's size
   */
  int getFileSize(String path);


  /**
   *  Sets a file's size to the given length
   *  Will delete data on shrink, and do nothing on grow
   */
  boolean truncate(String path, long size);

  /**
   *  Sets a file's mode to the given value
   */
  boolean chmod(String path, long mode);

  /**
   *  Set the UID / GID of the file for linux
   *  permissions.
   */
  boolean chown(String path, long uid, long gid);

  int open(String path, int flags);

  void initRootIfNeeded();
}
