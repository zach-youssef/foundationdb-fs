package foundationdb_fslayer.permissions;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

// ./AUTH
// Username -> Password Hash

// ./IDMAP
// Username -> UserId
public class PermissionManager {
    private static final List<String> ROOT_PATH = Arrays.asList(".");
    private static final List<String> AUTH_PATH = Arrays.asList(".", "AUTH");
    private static final List<String> ID_MAP_PATH = Arrays.asList(".", "IDMAP");
    private static final String ID_COUNTER_KEY = "ID_COUNTER";

    private static final long INITIAL_ID = 70000;

    private final long currentUserId;

    private PermissionManager(long id) {
        this.currentUserId = id;
    }

    public static Optional<PermissionManager> login(
            String username,
            String password,
            DirectoryLayer directoryLayer,
            Transaction transaction
            ) {
        byte[] passwordHash = hashPassword(password);
        if (passwordHash == null) {
            return Optional.empty();
        }
        try {
            // Open needed subspaces
            DirectorySubspace authSpace = directoryLayer.createOrOpen(transaction, AUTH_PATH).get();
            DirectorySubspace idSpace = directoryLayer.createOrOpen(transaction, ID_MAP_PATH).get();

            // Check if username exists in ./AUTH.
            byte[] storedHash = transaction.get(authSpace.pack(username)).get();
            if (storedHash != null) {
                // If so, compare to the hash of the given password
                if (!Arrays.equals(passwordHash, storedHash)) {
                    // If incorrect, fail
                    System.err.println("Incorrect password");
                    return Optional.empty();
                }
                // Otherwise, load the user id
                long id = Tuple.fromBytes(transaction.get(idSpace.pack(username)).get()).getLong(0);
                return Optional.of(new PermissionManager(id));
            } else {
                // Otherwise, add a new entry and generate a new id.
                return getNewUserId(directoryLayer, transaction).map(id -> {
                    transaction.set(idSpace.pack(username), Tuple.from(id).pack());
                    transaction.set(authSpace.pack(username), passwordHash);
                    return new PermissionManager(id);
                });
            }
        } catch (Exception e) {
            System.err.println("Error loading login information");
            e.printStackTrace();
            return Optional.empty();
        }
    }

    private static byte[] hashPassword(String password) {
        try {
            KeySpec spec = new PBEKeySpec(password.toCharArray(), new byte[16], 65536, 128);
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            return factory.generateSecret(spec).getEncoded();
        } catch (Exception e) {
            System.err.println("Hashing algorithm not found or spec invalid. All login attempts will fail.");
            e.printStackTrace();
            return null;
        }
    }

    private static Optional<Long> getNewUserId(DirectoryLayer directoryLayer, Transaction tr) {
        try {
            DirectorySubspace rootSpace = directoryLayer.open(tr, ROOT_PATH).get();
            byte[] rawCounterVal = tr.get(rootSpace.pack(ID_COUNTER_KEY)).get();
            long currentCount ;
            if (rawCounterVal == null) {
                currentCount = INITIAL_ID;
            } else {
                currentCount = Tuple.fromBytes(rawCounterVal).getLong(0);
            }
            long newUserId = currentCount + 1;
            tr.set(rootSpace.pack(ID_COUNTER_KEY), Tuple.from(newUserId).pack());
            return Optional.of(newUserId);
        } catch (Exception e) {
            System.err.println("Error computing new user id");
            e.printStackTrace();
            return Optional.empty();
        }
    }

    public long getId() {
        return this.currentUserId;
    }
}
