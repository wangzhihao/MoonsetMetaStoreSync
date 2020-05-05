package moonset.metastore.sync.util;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.collections.ListUtils;

public final class PathUtils {
    /**
     * Since checking against s3 directly is time-consuming, we check the folder/file by patterns.
     * Add new patterns here if needed.
     */
    private static final List<String> FILE_PATTERNS = Arrays.asList(".*\\.ion");

    private static final List<String> FOLDER_PATTERNS = Arrays.asList(".*/", ".*/[^/=]*=[^/=]*");

    private static final List<String> KNOWN_PATTERNS =
            ListUtils.union(FILE_PATTERNS, FOLDER_PATTERNS);

    /**
     * Trim the path to the parent directory if it is a file. Any path end with '/' is considered a
     * directory here.
     *
     * @path might be a directory or file.
     */
    public static String toDirectory(String path) {
        Matcher m = Pattern.compile("(.*)/[^/]*", Pattern.CASE_INSENSITIVE).matcher(path);
        if (m.matches()) return m.group(1);
        else return path;
    }

    /** Check if the path is a file, accroding to the known patterns. */
    public static boolean isFile(String path) {
        return isKnownPattern(path, FILE_PATTERNS);
    }

    /** Check if the path is a known pattern. */
    public static boolean isKnownPattern(String path) {
        return isKnownPattern(path, KNOWN_PATTERNS);
    }

    /** Helper method. */
    private static boolean isKnownPattern(String path, List<String> patterns) {
        for (String pattern : patterns) {
            Matcher m = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE).matcher(path);
            if (m.matches()) return true;
        }
        return false;
    }
}
