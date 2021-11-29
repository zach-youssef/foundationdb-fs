package foundationdb_fslayer;

import foundationdb_fslayer.fdb.object.Attr;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Util {
    public static List<String> parsePath(String path) {
        return Arrays.stream(path.split("/"))
                .filter(str -> !str.equals(""))
                .collect(Collectors.toList());
    }

    public static boolean checkPermission(long storedMode,
                                          long storedUser,
                                          long userId,
                                          long userMask,
                                          long otherMask) {
        System.out.printf("File Mode: %o\n", storedMode);
        long mask;
        if (storedUser == userId) {
            System.out.println("User matches");
            // If this user owns the file, compare the owner permissions:
            mask = userMask;
        } else {
            System.out.printf("User does not match: %d %d\n", storedUser, userId);
            // Otherwise, check the other permissions
            mask = otherMask;
        }
        long ans = storedMode & mask;
        System.out.printf("Permission calculated: %o\n", ans);
        return ans != 0;
    }
}
