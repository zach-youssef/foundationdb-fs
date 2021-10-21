import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.apple.foundationdb.FDB;
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
    FDB fdb = FDB.selectAPIVersion(630);
    fsLayer = new FoundationLayer(fdb);
    dir = new DirectoryLayer();
    list = List.of("alpha", "beta", "charlie");
  }

  @Test
  public void testHelloWorld() {
    assertEquals("Hello jess", fsLayer.helloWorld());
  }

  @Test
  public void testRmdir() {
    assertTrue(fsLayer.rmdir(dir, list.subList(0, 1)));
  }

  @Test
  public void testMkdir() {
    assertEquals("[alpha, beta, charlie]", fsLayer.mkdir(dir, list).getPath().toString());
  }

  @Test
  public void testLs() {
    assertEquals("[alpha]", fsLayer.ls(dir, list.subList(0, 0)).toString());
    assertEquals("[beta]", fsLayer.ls(dir, list.subList(0, 1)).toString());
    assertEquals("[charlie]", fsLayer.ls(dir, list.subList(0, 2)).toString());
    assertEquals("[]", fsLayer.ls(dir, list).toString());
  }
}
