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
                byte[] result = tr.get(Tuple.from("hello").pack()).join();
                return Tuple.fromBytes(result).getString(0);
            });

            output = hello;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }
}
