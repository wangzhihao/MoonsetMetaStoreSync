package moonset.metastore.sync;

import moonset.metastore.sync.catalog.CatalogClient;
import moonset.metastore.sync.exception.MetastoreConnectionException;
import moonset.metastore.sync.util.MetastoreRequestParamGenerator;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;

/**
 * Schedule tools like EDP, DJS need to check if the partition is ready, to trigger the following
 * EMR logic. This class provide a method to achieve this goal.
 */
public class DataCatalogPartitionSensor {

    private final IMetaStoreClient dataCatalogClient;

    /**
     * Construtor with a IMetaStoreClient.
     *
     * @param dataCatalogClient a IMetaStoreClient for the target metastore.
     */
    public DataCatalogPartitionSensor(final IMetaStoreClient dataCatalogClient) {
        this.dataCatalogClient = dataCatalogClient;
    }
    /**
     * Check if any new partition which need to sync from edx to s3.
     * @throws MetastoreConnectionException if connects AWS Data Catalog failure
     */
    public boolean hasPartitionsToSync(final String dbName, final String tableName) throws MetastoreConnectionException {
        try {
            CatalogClient client = new CatalogClient(dataCatalogClient);
            Map<String, String> parameters = client.getTableParameter(dbName, tableName);
            int lastConsolidateTime = Integer.parseInt(parameters.get("last_consolidate_time"));
            Partition partition = client.getLatestPartition(dbName, tableName);
            return partition.getCreateTime() > lastConsolidateTime;
        } catch (Exception e) {
            throw new MetastoreConnectionException("Connect Metastore in error", e);
        }
    }

    /**
     * Check if partitions by specified filter condition are ready or not.
     *
     * <p>The filter condition is similar to the format "foo1=1;foo2=2;foo3=3". If a filter
     * condition includes OR mode, we will implement cross product and breakdown it to
     * sub-conditions, and then check status for all sub-conditions. Only when all sub-conditions
     * matched, we will mark ready for this input condition.
     *
     * <p>For example as input "foo1=1;foo2=2,3,4;foo3=5,6", we will split it to 6 sub-condition
     * "foo1=1;foo2=2;foo3=5", "foo1=1;foo2=2;foo3=6", "foo1=1;foo2=3;foo3=5",
     * "foo1=1;foo2=3;foo3=6", "foo1=1;foo2=4;foo3=5", "foo1=1;foo2=4;foo3=6". Only we can find
     * partition existed for each of those 6 sub-conditions, we mark ready for input
     * "foo1=1;foo2=2,3,4;foo3=5,6".
     *
     * @param dbName the database name.
     * @param tableName the table name.
     * @param partVals a key value pairs representation of partition
     * @return if partition can be found by specified filter condition, return true, otherwise
     *     return false.
     * @throws MetastoreConnectionException if connects AWS Data Catalog failure
     */
    public boolean isPartitionReady(
            final String dbName, final String tableName, final Map<String, String> partVals)
            throws MetastoreConnectionException {
        try {
            // We suppose input args had already been verified in command line entrance
            List<String> partitionFilters =
                    MetastoreRequestParamGenerator.generatePartitionFilterList(partVals);

            for (String partitionFilter : partitionFilters) {
                if (!isPartitionReady(dbName, tableName, partitionFilter)) {
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
            throw new MetastoreConnectionException("Connect Metastore in error", e);
        }
    }

    /**
     * Check if partition is ready or not by a specified filter condition.
     *
     * <p>The partition filter condition is in string format that metastore client can recognize.
     * For example, we input "foo1=1;foo2=2;foo3=3" in command line, we need translated it to
     * foo1="1" AND foo2="2" AND foo3="3" before we invoke this method.
     *
     * @param dbName the database name.
     * @param tableName the table name.
     * @param partitionFilter a partition filter condition
     * @return if a partition can be found by specified filter condition, return true, otherwise
     *     return false.
     * @throws MetastoreConnectionException if connects AWS Data Catalog failure
     */
    public boolean isPartitionReady(
            final String dbName, final String tableName, final String partitionFilter)
            throws MetastoreConnectionException {
        try {
            List<Partition> partitions =
                    dataCatalogClient.listPartitionsByFilter(
                            dbName, tableName, partitionFilter, (short) -1);

            if (CollectionUtils.isEmpty(partitions)) {
                return false;
            }

            return true;
        } catch (Exception e) {
            throw new MetastoreConnectionException("Connect Metastore in error", e);
        }
    }
}
