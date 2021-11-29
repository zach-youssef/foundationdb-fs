package foundationdb_fslayer;

import foundationdb_fslayer.fdb.FoundationFileOperations;
import foundationdb_fslayer.fdb.FoundationLayer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.Assert.*;

public class fdbTest {

  private FoundationFileOperations fsLayer;
  private static final String testPath = "/junit_test";

  @Before
  public void setup() {
    fsLayer = new FoundationLayer(630);
  }

  @BeforeEach
  public void prepareTest() {
    fsLayer.rmdir(testPath);
    fsLayer.mkdir(testPath);
  }

  @AfterEach
  public void cleanup() {
    fsLayer.rmdir(testPath);
  }

  @Test
  public void testRead() {
    // Write some data to test read
    fsLayer.createFile("/junit_test/hello", );
    fsLayer.write("/junit_test/hello", "world".getBytes());

    // Assert the read is correct
    assertEquals("world", new String(fsLayer.read("/junit_test/hello")));

    // Clean up the file
    fsLayer.clearFileContent("/junit_test/hello");
  }


  @Test
  public void testClearFileContent() {
    // Create a file to delete
    fsLayer.createFile("/junit_test/delete_me", );
    fsLayer.write("/junit_test/delete_me", new byte[1], 0);

    // Delete the file
    fsLayer.clearFileContent("/junit_test/delete_me");

    // Verify the file no longer exists
    assertNull(fsLayer.read("/junit_test/delete_me"));
  }

  @Test
  public void testWrite() {
    // create new file
    String filePath = "/junit_test/file";
    fsLayer.createFile(filePath, );

    // Write to file
    String startPhrase = "start writing to file";
    byte[] startPhraseBytes = startPhrase.getBytes(StandardCharsets.UTF_8);
    fsLayer.write(filePath, startPhraseBytes, 0);
    // Verify the output
    assertEquals(startPhrase, new String(fsLayer.read(filePath)));

    // continue writing to file
    String continuePhrase = " Continue writing to file";
    fsLayer.write(filePath, continuePhrase.getBytes(StandardCharsets.UTF_8), startPhraseBytes.length);
    String fileContent = startPhrase + continuePhrase;
    // Verify the new string has been appended
    assertEquals(fileContent, new String(fsLayer.read(filePath)));


    //Cleanup file
    fsLayer.clearFileContent(filePath);
  }

  @Test
  public void testReadOffsetSize(){
    // create new file
    String filepath = "/file";
    fsLayer.createFile(filepath, );

    // Write to the file
    String data = "We want to read just THIS, not anything else";
    fsLayer.write(filepath, data.getBytes(StandardCharsets.UTF_8));

    System.out.println(new String(fsLayer.read(filepath)));

    // Read just THIS
    //assertEquals(new String("THIS".getBytes(StandardCharsets.UTF_8)), new String(fsLayer.read(filepath, 21, 4)));

    // cleanup
    //fsLayer.clearFileContent(filepath);
  }


  @Test
  public void testRmdir() {
    // Verify deletion is successful
    assertTrue(fsLayer.rmdir(testPath));
    // Verify the directory is actually deleted
    assertNull(fsLayer.ls(testPath));
  }

  @Test
  public void testMkdir() {
    // Create a new directory
    String newPath = testPath + "/mkdir";
    fsLayer.mkdir(newPath);

    // Verify the new directory exists
    assertNotNull(fsLayer.ls(newPath));
  }

  @Test
  public void testLs() {
    // Create some subdirectories
    List<String> subDirNames = Arrays.asList("alpha", "bravo", "charlie");
    for (String subDirName : subDirNames){
      fsLayer.mkdir(testPath + "/" + subDirName);
    }

    // Create some files
    List<String> filenames = Arrays.asList("a.txt", "b.png", "c.mp4");
    for (String filename : filenames) {
      String filepath = testPath + "/" + filename;
      fsLayer.createFile(filepath, );
      fsLayer.write(filepath, new byte[1],0);
    }

    // Call ls
    List<String> lsOut = fsLayer.ls(testPath);

    // Verify all the created items are present
    filenames.forEach(filename -> assertTrue(lsOut.contains(filename)));
    subDirNames.forEach(subDirName -> assertTrue(lsOut.contains(subDirName)));
  }
}
