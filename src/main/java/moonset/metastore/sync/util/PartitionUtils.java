package moonset.metastore.sync.util;

import moonset.metastore.sync.exception.MetastoreException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.Partition;

public final class PartitionUtils {
    /**
     * AWS DataCatalog allow partition location to be a file, but Hive Metastore only allows a
     * directory. Thus we need to trim any file to its parent directory. The function would not be
     * affected.
     *
     * @param partitions the original partitions
     * @return the partitions which only contain directory location
     */
    public static List<Partition> rewritePartitionLocation(List<Partition> partitions)
            throws MetastoreException {
        for (Partition partition : partitions) {
            String location = partition.getSd().getLocation();
            if (!PathUtils.isKnownPattern(location)) {
                throw new MetastoreException(
                        "The location " + location + " has an unknown pattern.");
            }
        }
        return partitions
                .stream()
                .map(
                        partition -> {
                            Partition modifiedPartition = partition.deepCopy();
                            String location = modifiedPartition.getSd().getLocation();
                            if (PathUtils.isFile(location)) {
                                modifiedPartition
                                        .getSd()
                                        .setLocation(PathUtils.toDirectory(location));
                            }
                            return modifiedPartition;
                        })
                .collect(Collectors.toList());
    }
    public static List<Partition> addOriginalCreateTime(List<Partition> partitions)
            throws MetastoreException {
        return partitions
                .stream()
                .map(
                        partition -> {
                            Partition modifiedPartition = partition.deepCopy();
                            if(!modifiedPartition.getParameters().containsKey(TableUtils.ORIGINAL_CREATE_TIME)) {
                                modifiedPartition.getParameters().put(TableUtils.ORIGINAL_CREATE_TIME, String.valueOf(modifiedPartition.getCreateTime()));
                            }
                            return modifiedPartition;
                        })
                .collect(Collectors.toList());
    }

    public static List<Partition> updateDatabaseAndTableName(List<Partition> rewritedPartitions, String destDatabaseName, String destTableName) {
        rewritedPartitions.stream().forEach(p -> {
            p.setDbName(destDatabaseName);
            p.setTableName(destTableName);
        });
        return rewritedPartitions;
    }
}
