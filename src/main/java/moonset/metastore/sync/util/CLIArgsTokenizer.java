package moonset.metastore.sync.util;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public final class CLIArgsTokenizer {
    public static final String PARTITION_ENTRY_SEPARATOR = ";";
    public static final String PARTITION_KEY_SEPARATOR = "=";
    public static final String PARTITION_VALUE_SEPARATOR = ",";

    /**
     * @param partitionArg partition format is
     *     region_id=foo;marketplace_id=foo;snapshot_day=foo1,foo2,foo3
     * @return Map<String, String>
     */
    public static Map<String, String> parsePartition(final String partitionArg) {
        if (StringUtils.isBlank(partitionArg)) {
            throw new IllegalArgumentException(
                    "Specify partition as input parameter, but set it to empty wrongly.");
        }

        String[] partEntries = partitionArg.split(PARTITION_ENTRY_SEPARATOR);
        if (partEntries.length == 0) {
            throw new IllegalArgumentException("Empty partition provided");
        }

        Map<String, String> partitionValueMap = Maps.newHashMap();

        for (String partEntryStr : partEntries) {
            if (StringUtils.isBlank(partEntryStr)
                    || !partEntryStr.contains(PARTITION_KEY_SEPARATOR)) {
                throw new IllegalArgumentException("Invalid partition key/value pair provided");
            }

            String[] partEntryPair = partEntryStr.split(PARTITION_KEY_SEPARATOR);
            if (2 != partEntryPair.length) {
                throw new IllegalArgumentException(
                        "Invalid partition key/value pair provided with 2+ "
                                + PARTITION_KEY_SEPARATOR);
            }

            String partitionKey = partEntryPair[0];
            String partitionValue = partEntryPair[1];
            if (StringUtils.isBlank(partitionKey) || StringUtils.isBlank(partitionValue)) {
                throw new IllegalArgumentException("Empty key/value provided in partition entry");
            }

            if (partitionValue.contains(PARTITION_VALUE_SEPARATOR)) {
                String[] partValues = partitionValue.split(PARTITION_VALUE_SEPARATOR);
                for (String partValue : partValues) {
                    if (StringUtils.isBlank(partValue)) {
                        throw new IllegalArgumentException("Empty partition value provided");
                    }
                }
            }
            // We will delay value list parser to build metastore partition query because we don't know how to build the expression yet
            partitionValueMap.put(partitionKey, partitionValue);
        }

        return partitionValueMap;
    }
}
