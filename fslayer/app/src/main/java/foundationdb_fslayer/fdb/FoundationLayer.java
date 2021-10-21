package foundationdb_fslayer.fdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;

public class FoundationLayer implements FoundationFileOperations {
    private final FDB fdb;

    public FoundationLayer(FDB fdb){
        this.fdb = fdb;
    }

    @Override
    public String HelloWorld() {
        String output = " ERROR" ;
        try(Database db = fdb.open()) {
            // Run an operation on the database
            db.run(tr -> {
                tr.set(Tuple.from("hello").pack(), Tuple.from("world").pack());
                return null;

            });

            // Get the value of 'hello' from the database
            String hello = db.run(tr -> {
                byte[] result = tr.get(Tuple.from("write/op").pack()).join();
                return Tuple.fromBytes(result).getString(0);
            });

            output = hello;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    /* Baisc Write operation to write at the end of a file
    * @param path, path of the file
    * @param data, content that is appended to the file
    * If file path does not already exist, file path is created
    * */
    @Override
    public void write(String path, String data) {
        try(Database db = fdb.open()) {

            // Get existing value of path from the database
            byte[] buffer = db.run(tr -> {
                byte[] buf = tr.get(Tuple.from(path).pack()).join();
                return buf;
            });

            // add data to existing and set to path
            if(buffer != null) {
                db.run(tr -> {
                    String content = Tuple.fromBytes(buffer).getString(0);
                    tr.set(Tuple.from(path).pack(), Tuple.from( content + data).pack());
                    return null;
                });
            } else {
                db.run(tr -> {
                    tr.set(Tuple.from(path).pack(), Tuple.from( data).pack());
                    return null;
                });
            }
            // For dev use
            String output = db.run(tr -> {
                byte[] buf = tr.get(Tuple.from(path).pack()).join();
                return Tuple.fromBytes(buf).getString(0);
            });
            System.out.println(output);





        } catch (Exception e) {
            e.printStackTrace();
        }




    }
}
