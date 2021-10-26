package foundationdb_fslayer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.tuple.Tuple;
import foundationdb_fslayer.fdb.FoundationFileOperations;
import foundationdb_fslayer.fdb.FoundationLayer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class fdbTest {

  private FoundationFileOperations fsLayer;
  private Directory dir;
  private List<String> list;

  @Before
  public void setup() {
    fsLayer = new FoundationLayer(630);
    dir = new DirectoryLayer();
    //list = List.of("alpha", "beta", "charlie"); // dir: alpha/beta/charlie/

  }

  @Test
  public void testHelloWorld() {
    assertEquals("world", fsLayer.helloWorld());
  }

  @Test
  public void testRead() {
    assertEquals("world", Tuple.fromBytes(fsLayer.read("hello")).getString(0));
  }


  @Test
  public void testClearFileContent() {
    fsLayer.clearFileContent("hello");
    assertTrue(fsLayer.read("hello") == null);
  }

  @Test
  public void testWrite() {
    // create new file
    String filePath = "alpha/beta/file";

    String startPhrase = "start writing to file";
    // Write to file
    fsLayer.write(filePath, startPhrase.getBytes(StandardCharsets.UTF_8));
    assertEquals(startPhrase, new String(fsLayer.read(filePath)));

    // continue writing to file
    String continuePhrase = " Continue writing to file";
    fsLayer.write(filePath, continuePhrase.getBytes(StandardCharsets.UTF_8));
    String fileContent = startPhrase + continuePhrase;
    assertEquals(fileContent, new String(fsLayer.read(filePath)));

    // clear contents of file
    fsLayer.clearFileContent(filePath);

  }


  @Test
  public void testRmdir() {
    fsLayer.mkdir(dir, list);

    assertTrue(fsLayer.rmdir(dir, list.subList(0, 1)));
  }

  @Test
  public void testMkdir() {
    assertEquals("[alpha, beta, charlie]", fsLayer.mkdir(dir, list).getPath().toString());
  }

  @Test
  public void testLs() {
    fsLayer.mkdir(dir, list);

    assertEquals("[alpha]", fsLayer.ls(dir, list.subList(0, 0)).toString()); // ls /
    assertEquals("[beta]", fsLayer.ls(dir, list.subList(0, 1)).toString()); // ls alpha/
    assertEquals("[charlie]", fsLayer.ls(dir, list.subList(0, 2)).toString()); // ls alpha/beta/
    assertEquals("[]", fsLayer.ls(dir, list).toString()); // ls alpha/beta/charlie/

    fsLayer.rmdir(dir, list);
  }
}
