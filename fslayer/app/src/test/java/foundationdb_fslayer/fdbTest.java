package foundationdb_fslayer;

import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import foundationdb_fslayer.fdb.FoundationFileOperations;
import foundationdb_fslayer.fdb.FoundationLayer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.Assert.*;

public class fdbTest {

  private FoundationFileOperations fsLayer;
  private Directory dir;
  private List<String> testDirPath;

  @Before
  public void setup() {
    fsLayer = new FoundationLayer(630);
    dir = new DirectoryLayer();
    testDirPath = new ArrayList<>();
    testDirPath.add("junit_test");
  }

  @BeforeEach
  public void prepareTest() {
    fsLayer.rmdir(dir, testDirPath);
    fsLayer.mkdir(dir, testDirPath);
  }

  @AfterEach
  public void cleanup() {
    fsLayer.rmdir(dir, testDirPath);
  }

  @Test
  public void testRead() {
    // Write some data to test read
    fsLayer.write("/junit_test/hello", "world".getBytes());

    // Assert the read is correct
    assertEquals("world", new String(fsLayer.read("/junit_test/hello")));

    // Clean up the file
    fsLayer.clearFileContent("/junit_test/hello");
  }


  @Test
  public void testClearFileContent() {
    // Create a file to delete
    fsLayer.write("/junit_test/delete_me", new byte[1]);

    // Delete the file
    fsLayer.clearFileContent("/junit_test/delete_me");

    // Verify the file no longer exists
    assertNull(fsLayer.read("/junit_test/delete_me"));
  }

  @Test
  public void testWrite() {
    // create new file
    String filePath = "/junit_test/file";

    // Write to file
    String startPhrase = "start writing to file";
    fsLayer.write(filePath, startPhrase.getBytes(StandardCharsets.UTF_8));
    // Verify the output
    assertEquals(startPhrase, new String(fsLayer.read(filePath)));

    // continue writing to file
    String continuePhrase = " Continue writing to file";
    fsLayer.write(filePath, continuePhrase.getBytes(StandardCharsets.UTF_8));
    String fileContent = startPhrase + continuePhrase;
    // Verify the new string has been appended
    assertEquals(fileContent, new String(fsLayer.read(filePath)));
  }


  @Test
  public void testRmdir() {
    // Verify deletion is successful
    assertTrue(fsLayer.rmdir(dir, testDirPath));
    // Verify the directory is actually deleted
    assertNull(fsLayer.ls(dir, testDirPath));
  }

  @Test
  public void testMkdir() {
    // Create a new directory
    List<String> newPath = new ArrayList<>(testDirPath);
    newPath.add("mkdir");
    fsLayer.mkdir(dir, newPath);

    // Verify the new directory exists
    assertNotNull(fsLayer.ls(dir, newPath));
  }

  @Test
  public void testLs() {
    // Create some subdirectories
    List<String> subDirNames = Arrays.asList("alpha", "bravo", "charlie");
    for (String subDirName : subDirNames){
      List<String> path = new ArrayList<>(testDirPath);
      path.add(subDirName);
      fsLayer.mkdir(dir, path);
    }

    // Create some files
    List<String> filenames = Arrays.asList("a.txt", "b.png", "c.mp4");
    for (String filename : filenames) {
      fsLayer.write("/junit_test/" + filename, new byte[1]);
    }

    // Call ls
    List<String> lsOut = fsLayer.ls(dir, testDirPath);

    // Verify all the created items are present
    filenames.forEach(filename -> assertTrue(lsOut.contains(filename)));
    subDirNames.forEach(subDirName -> assertTrue(lsOut.contains(subDirName)));
  }
}
