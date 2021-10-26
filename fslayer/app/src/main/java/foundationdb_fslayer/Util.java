package foundationdb_fslayer;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Util {
    public static List<String> parsePath(String path) {
        return Arrays.stream(path.split("/"))
                .filter(str -> !str.equals(""))
                .collect(Collectors.toList());
    }
}
