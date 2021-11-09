package foundationdb_fslayer.fdb.object;

/**
 * A unique identifier for Opened File
 * TODO: Add inode, flag
 */
public class FileHandle {
  private String filename;
  private int referenceCounter;

  public FileHandle(String filename, int referenceCounter) {
    this.filename = filename;
    this.referenceCounter = referenceCounter;

  }

  public String getFilename() {
    return filename;
  }

  public int getReferenceCounter() {
    return referenceCounter;
  }

  public void setReferenceCounter(int referenceCounter) {
    this.referenceCounter = referenceCounter;
  }
}