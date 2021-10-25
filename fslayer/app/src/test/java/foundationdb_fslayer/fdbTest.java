package foundationdb_fslayer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import foundationdb_fslayer.fdb.FoundationFileOperations;
import foundationdb_fslayer.fdb.FoundationLayer;
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
    list = List.of("alpha", "beta", "charlie"); // dir: alpha/beta/charlie/
  }

  @Test
  public void testHelloWorld() {
    assertEquals("world", fsLayer.helloWorld());
  }

  @Test
  public void testRead() {
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
