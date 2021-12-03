package foundationdb_fslayer.fdb;

import com.apple.foundationdb.directory.DirectorySubspace;
import foundationdb_fslayer.fdb.object.Attr;
import foundationdb_fslayer.permissions.PermissionManager;

import java.util.List;
import java.util.Optional;

public interface FoundationFileOperations {
  /**
   * Read a file.
   *
   * @param path file path
   * @param userId
   * @return encoded byte representation of the file content
   */
  byte[] read(String path, long offset, long size, long userId);

  default byte[] read(String path, long userId){
    return read(path, 0, getFileSize(path), userId);
  }

  /**
   * Write to a file
   * @param path file path
   * @param data data to be added to file
   * @param userId
   * @return
   */
  boolean write(String path, byte[] data, long offset, long userId);

  default boolean write(String path, byte[] data, int version){
    return write(path, data, 0, version);
  }

  /**
   * Remove an empty directory if exists.
   *
   * @param path list of path strings
   * @return a boolean value determining whether removing the directory is successful
   */
  boolean rmdir(String path, long uid);

  /**
   * Create a new directory matching the provided path under the given directory.
   *
   * @param path list of path strings
   * @return The directory subspace of the newly created directory
   */
  DirectorySubspace mkdir(String path, long mode, long uid);

  /**
   * List all directories under the provided directory.
   *
   * @param path list of path strings
   * @return a list of strings representing all sub-directories under the current directory
   */
  List<String> ls(String path, long userId);

  /**
   * Clear content of file
   *
   * @param file file path
   */
  boolean clearFileContent(String file, long userId);

  /**
   * Creates a new empty file at the given path
   * returns false on failure
   */
  boolean createFile(String path, long userId);


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
  boolean truncate(String path, long size, long userId);

  /**
   *  Sets a file's mode to the given value
   */
  boolean chmod(String path, long mode, long userId);

  /**
   *  Set the UID / GID of the file for linux
   *  permissions.
   */
  boolean chown(String path, long uid, long gid);

  int open(String path, int flags);

  boolean move(String oldPath, String newPath, long userId);

  void initRootIfNeeded();

  Optional<PermissionManager> login(String username, String password);

}
