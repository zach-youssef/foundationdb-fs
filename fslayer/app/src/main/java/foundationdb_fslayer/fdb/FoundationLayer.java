package foundationdb_fslayer.fdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;

public class FoundationLayer implements FoundationFileOperations {
    private final FDB fdb;
    private final Database db;

    public FoundationLayer(FDB fdb){
        this.fdb = fdb;
        this.db = fdb.open();
    }

    @Override
    public String HelloWorld() {
        String output = " ERROR" ;
        try {
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

    /**
     * Read value for specific path from the database
     * @param path - String
     * @return - byte array
     */
    @Override
    public byte[] read(String path) {
        return db.read(tr -> tr.get(Tuple.from(path).pack()).join());
    }
}
