package moonset.metastore.sync;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import moonset.metastore.sync.catalog.CatalogClient;
import moonset.metastore.sync.exception.MetastoreException;
import moonset.metastore.sync.util.PartitionUtils;
import moonset.metastore.sync.util.TableUtils;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

/** The utility class for metastore sync process. */
@Slf4j
public final class MetastoreSyncUtils {

    /** Prevent the class to be instanced. */
    private MetastoreSyncUtils() {}

    /** Batch threshold for add_partitions() method. */
    private static final int BATCH_SIZE = 30;

    /** Sync all partitions of the target name from source metastore to dest metastore. */
    public static void syncAllPartitions(
            final IMetaStoreClient source,
            final IMetaStoreClient dest,
            final String srcDatabaseName,
            final String srcTableName,
            final String destDatabaseName,
            final String destTableName)
            throws MetastoreException {
        CatalogClient client = new CatalogClient(source);
        syncPartitions(dest, destDatabaseName, destTableName, client.getAllPartitions(srcDatabaseName, srcTableName));
    }

    /**
     * Sync partitions which match the patterns in <code>partVals</code> of the target table from
     * source metastore to dest metastore.
     */
    public static void syncPartitions(
            final IMetaStoreClient source,
            final IMetaStoreClient dest,
            final String srcDatabaseName,
            final String srcTableName,
            final String destDatabaseName,
            final String destTableName,
            final Map<String, String> partVals)
            throws MetastoreException {
        List<Partition> partitions = null;
        // We don't use pagination query since partition size is not large enough, and we will
        // need not return partition to client
        if (MapUtils.isEmpty(partVals)) {
            partitions = Lists.newArrayList();
        } else {
            // Only load specify partitions
            CatalogClient client = new CatalogClient(source);
            partitions = client.getPartitions(srcDatabaseName, srcTableName, partVals);
        }
        syncPartitions(dest, destDatabaseName, destTableName, partitions);
    }

    /**
     * Sync table across metastores, such as from daylight to hive meatastore. Notice the dbName and
     * tableName should not contain dot(.) since hive metastore will reject it.  If the table exists
     * in destination, we only update table properties to avoid to damage the destination table's structure.
     * Some use cases want to store some variable inside table properties.
     */
    public static void syncTable(
            final IMetaStoreClient source,
            final IMetaStoreClient dest,
            final String srcDatabaseName,
            final String srcTableName,
            final String destDatabaseName,
            final String destTableName)
            throws MetastoreException {
        try {
            try {
                // Validate if dest database already exists
                dest.getDatabase(destDatabaseName);
                log.info("The database " + destDatabaseName + " already exists in destination metastore, and we need not sync it again.");
            } catch (NoSuchObjectException nsoe) {
                log.info("The database " + destDatabaseName + " doesn't exist in destination metastore, and start to sync it.");

                // Create destination database if it doesn't exist
                Database database = source.getDatabase(srcDatabaseName);
                log.trace("The database: " + database);

                // Rename database to destDatabaseName.
                database.setName(destDatabaseName);

                // Fix error: java.lang.IllegalArgumentException: Can not create a Path from an empty
                // We will rewrite the location url regard less it is empty, s3 or hdfs
                StringBuilder rewriteUri = new StringBuilder("hdfs:///locationrewriteuri/");
                rewriteUri.append(destDatabaseName).append(".db");
                log.info("rewrite location uri from " + database.getLocationUri() + " to hdfs location " + rewriteUri.toString());
                database.setLocationUri(rewriteUri.toString());
                dest.createDatabase(database);
                log.info("The database " + destDatabaseName + " is created successfully.");
            }

            if (dest.tableExists(destDatabaseName, destTableName)) {
                log.info("The table " + destTableName + " already exists in destination metastore, and we only need to sync table properties.");
                Table destTable = dest.getTable(destDatabaseName, destTableName);
                log.info(String.format("The table original properties: %s", destTable.getParameters()));
                Map<String, String> parameters = destTable.getParameters();
                parameters.putAll(source.getTable(srcDatabaseName, srcTableName).getParameters());
                log.info(String.format("The table properties after updated: %s", parameters));
                destTable.setParameters(parameters);
                dest.alter_table(destDatabaseName, destTableName, destTable);
                log.info("The table properties updated successfully.");
            } else {
                log.info("The table " + destTableName + " doesn't exist in destination metastore, and start to sync it.");
                Table sourceTable = source.getTable(srcDatabaseName, srcTableName);
                log.info("Rewrite the table EXTERNAL paramater, which is used by Hive Metastore.");
                Table modifiedTable = TableUtils.rewriteExternalFlag(sourceTable);
                // Rename table name to destTableName.
                modifiedTable.setDbName(destDatabaseName);
                modifiedTable.setTableName(destTableName);

                //Store the create time in table parameter, sice this field will be overriden when createTable().
                if(!modifiedTable.getParameters().containsKey(TableUtils.ORIGINAL_CREATE_TIME)) {
                    modifiedTable.getParameters().put(TableUtils.ORIGINAL_CREATE_TIME, String.valueOf(modifiedTable.getCreateTime()));
                }
                dest.createTable(modifiedTable);
                log.trace("The table: " + modifiedTable);
                log.info("The table " + destTableName + " is created successfully.");
            }

            //validate dest table
            if (!"TRUE".equals(dest.getTable(destDatabaseName, destTableName).getParameters().get(TableUtils.EXTERNAL_PARAM))) {
                throw new MetastoreException(
                        "It's dangerous to process a non-external table. Prohibit to do so.");
            }
        } catch (TException e) {
            throw new MetastoreException("failed to sync table", e);
        }
    }

    /**
     * The partitions should be processed in small batch, since the server creates a thread for each
     * partition and make a transaction for each call. Without traffic control, too much threads
     * will be created and overwhelm the server.
     */
    public static void syncPartitions(
            final IMetaStoreClient dest,
            final String destDatabaseName,
            final String destTableName,
            final List<Partition> partitions)
            throws MetastoreException {
        try {
            if (CollectionUtils.isEmpty(partitions)) {
                // We only log empty partitions but do nothing
                log.info("No partition found, skip sync partitions.");
            } else {
                log.info("There are " + partitions.size() + " partitions needed to sync.");

                log.info(
                        "Rewrite the partition location to directory if any file location exists.");
                List<Partition> rewritedPartitions =  PartitionUtils.rewritePartitionLocation(partitions);
                log.info("Rewrite complete.");

                log.info("Update database name and table name to dest database and dest table.");
                List<Partition> rewritedPartitions2 =  PartitionUtils.updateDatabaseAndTableName(rewritedPartitions, destDatabaseName, destTableName);
                log.info("Database name and table name updated.");

                log.info("Add original create time to partitions.");
                List<Partition> rewritedPartitions3 =  PartitionUtils.addOriginalCreateTime(rewritedPartitions2);
                log.info("Original create time added.");



                log.info("Begin to sync partitions.");
                for (int i = 0; i < rewritedPartitions3.size(); i += BATCH_SIZE) {
                    List<Partition> sublistPartitions =
                            rewritedPartitions3.subList(i, Math.min(i + BATCH_SIZE, rewritedPartitions3.size()));
                    log.trace("The partitions: " + sublistPartitions);
                    dest.add_partitions(sublistPartitions, true, false);
                    log.info(
                            "The range [ "
                                    + i
                                    + ", "
                                    + Math.min(i + BATCH_SIZE, rewritedPartitions3.size())
                                    + " ) partitions have been synced.");
                }
            }
        } catch (TException e) {
            throw new MetastoreException("failed to sync table", e);
        }
    }
}
