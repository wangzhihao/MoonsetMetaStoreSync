package moonset.metastore.sync.util;

import static moonset.metastore.sync.util.CLIArgsTokenizer.PARTITION_VALUE_SEPARATOR;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

public final class MetastoreRequestParamGenerator {

    /**
     * Generation partition filter string.
     *
     * <p>If input condition includes OR condition, we will combine them into a single filter string
     *
     * @param partVals
     * @return
     */
    public static String generateCompositePartitionFilter(final Map<String, String> partVals) {
        if (MapUtils.isEmpty(partVals)) {
            throw new IllegalArgumentException("Empty partition parameters input");
        }

        final String AND = " AND ";
        final String OR = " OR ";

        StringBuilder filter = new StringBuilder();
        for (Map.Entry<String, String> entry : partVals.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // We had already verified each key/value pair in program entrance to make sure it is not empty
            // So, we will not check it again
            if (!value.contains(PARTITION_VALUE_SEPARATOR)) {
                filter.append(key).append("=").append("\"").append(value).append("\"").append(AND);
            } else {
                filter.append(" ( ");
                String[] subConditions = value.split(PARTITION_VALUE_SEPARATOR);
                int subCondSize = subConditions.length;
                for (int i = 0; i < subCondSize; i++) {
                    String subCond = subConditions[i];
                    filter.append(key).append("=").append("\"").append(subCond).append("\"");
                    if (i < subCondSize - 1) {
                        filter.append(OR);
                    }
                }
                filter.append(" ) ").append(AND);
            }
        }

        int length = filter.toString().length();
        if (length > 0) filter.delete(length - AND.length(), length);

        return filter.toString();
    }

    /**
     * Generation partition filter string list.
     *
     * <p>If input condition includes OR condition, we will separate it to several partition string
     *
     * @param partVals
     * @return
     */
    public static List<String> generatePartitionFilterList(Map<String, String> partVals) {
        if (MapUtils.isEmpty(partVals)) {
            throw new IllegalArgumentException("Empty partition parameters input");
        }

        List<String> partitionFilters = Lists.newArrayList();

        for (Map.Entry<String, String> entry : partVals.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // We had already verified each key/value pair in program entrance to make sure it is not empty
            // So, we will not check it again
            List<String> conditionList = Lists.newArrayList();
            if (!value.contains(PARTITION_VALUE_SEPARATOR)) {
                StringBuilder filter = new StringBuilder();
                filter.append(key).append("=").append("\"").append(value).append("\"");
                conditionList.add(filter.toString());
            } else {
                String[] subConditions = value.split(PARTITION_VALUE_SEPARATOR);
                for (String subCond : subConditions) {
                    StringBuilder filter = new StringBuilder();
                    filter.append(key).append("=").append("\"").append(subCond).append("\"");
                    conditionList.add(filter.toString());
                }
            }

            partitionFilters = joinPartitionConditions(partitionFilters, conditionList);
        }

        return partitionFilters;
    }

    private static List<String> joinPartitionConditions(
            List<String> firstConditionList, List<String> secondConditionList) {
        if (CollectionUtils.isEmpty(firstConditionList)) {
            return secondConditionList;
        }

        if (CollectionUtils.isEmpty(secondConditionList)) {
            return firstConditionList;
        }

        List<String> joinedFilters = Lists.newArrayList();

        final String AND = " AND ";

        for (String leftFilter : firstConditionList) {
            for (String rightFilter : secondConditionList) {
                StringBuilder filter = new StringBuilder();
                filter.append(leftFilter).append(AND).append(rightFilter);
                joinedFilters.add(filter.toString());
            }
        }

        return joinedFilters;
    }
}
