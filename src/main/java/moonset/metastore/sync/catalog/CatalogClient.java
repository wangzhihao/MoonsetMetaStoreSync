package moonset.metastore.sync.catalog;

import moonset.metastore.sync.exception.MetastoreException;
import moonset.metastore.sync.util.MetastoreRequestParamGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

/**
 * A simple Catalog client to manipulate metastore table information. Similar to HCatClient.java in Hive.
 */
@Slf4j
public class CatalogClient {
    private static final Log log =
        LogFactory.getLog(CatalogClient.class);


    private IMetaStoreClient client;

    public CatalogClient(IMetaStoreClient client) {
        this.client = client;
    }
    public List<FieldSchema> getPartitionColumns(String dbName, String tableName) throws MetastoreException {
        try {
         Table t = client.getTable(dbName, tableName);
         return t.getPartitionKeys();
        } catch (TException e) {
            throw new MetastoreException("fail to get table partition columns.", e);
        }
     }
    public Map<String, String> getPartitionSpecs(Partition partition) throws MetastoreException {
        List<FieldSchema> partitionColumns = getPartitionColumns(partition.getDbName(), partition.getTableName());
        Map<String, String> partitionSpecs = Maps.newHashMap();
        for(int index = 0; index < partitionColumns.size(); index++){
            String colName = partitionColumns.get(index).getName();
            String colVal = partition.getValues().get(index);
            partitionSpecs.put(colName, colVal);
        }
        return partitionSpecs;
     }

    public List<Partition> getPartitions(String database, String table, Map<String, String> partVals) throws MetastoreException {
        log.trace(String.format("The input arguments: database %s, table %s, partVals %s.", database, table, partVals));
        if(client instanceof HiveMetaStoreClient) {
            log.info("Pull all partitions and filter local side for HiveMetaStoreClient, since HiveMetaStoreClient.listPartitionsByFilter can " +
                     "only filter string type when hive.metastore.intergal.jdo.pushdown disabled, and case senstive when hive.metastore.intergal.jdo.pushdown enabled, " +
                     "both of them undesired.");
            List<Partition> partitions = Lists.newArrayList();
            List<Partition> allPartitions = getAllPartitions(database, table);
            log.debug(String.format("There are %s candidates partition", allPartitions.size()));
            log.trace(String.format("The candidate partitions: %s", allPartitions));
            for(Partition partition : allPartitions) {
                Map<String, String> partitionSpecs = getPartitionSpecs(partition);
                log.trace(String.format("The partition spec: %s", partitionSpecs));
                if(caseInsensitive(partitionSpecs).entrySet().containsAll(caseInsensitive(partVals).entrySet())) {
                    partitions.add(partition);
                }
            }
            return partitions;
        } else {
            try {
                // Only load specify partitions
                return client.listPartitionsByFilter(
                    database,
                    table,
                    MetastoreRequestParamGenerator.generateCompositePartitionFilter(partVals),
                    (short) -1);
            } catch(TException e) {
                throw new MetastoreException("fail to get table partitions.", e);
            }
        }
    }
    /**
     * Make a map's key case insensitive.
     */
    private Map<String, String> caseInsensitive(Map<String, String> map) {
        Map<String, String> result = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        result.putAll(map);
        return result;
    }

    public void setTableParameter(String database, String table, Map<String, String> parameters) throws MetastoreException {
        try {
            Table t = client.getTable(database, table);
            t.setParameters(parameters);
            client.alter_table(database, table, t);
        } catch (TException e) {
            throw new MetastoreException("fail to set table parameters.", e);
        }
    }

    public Map<String, String> getTableParameter(String database, String table) throws MetastoreException {
        try {
            return client.getTable(database, table).getParameters();
        } catch (TException e) {
            throw new MetastoreException("fail to get table parameters.", e);
        }

    }
    public List<Partition> getAllPartitions(String database, String table) throws MetastoreException {
        try {
            return client.listPartitions(database, table, (short) -1); // -1 means get all partitions.
        } catch (TException e) {
            throw new MetastoreException("fail to sync partitions .", e);
        }
    }

    /**
     * Get the oldest partition of the given table. If no partition return null.
     */
    public Partition getOldestPartition(String database, String table) throws MetastoreException {
        List<Partition> partitions = getAllPartitions(database, table);
        Partition result = null;
        for (Partition partition : partitions) {
            if (result == null || result.getCreateTime() > partition.getCreateTime())
                result = partition;
        }
        return result;
    }

    /**
     * Get the latest partition of the given table. If no partition return null.
     */
    public Partition getLatestPartition(String database, String table) throws MetastoreException {
        List<Partition> partitions = getAllPartitions(database, table);
        Partition result = null;
        for (Partition partition : partitions) {
            if (result == null || result.getCreateTime() < partition.getCreateTime())
                result = partition;
        }
        return result;
    }

    /**
     * Drop partition which matches the partition spec.
     * Copy from https://github.com/apache/hive/blob/master/hcatalog/webhcat/java-client/src/main/java/org/apache/hive/hcatalog/api/HCatClientHMSImpl.java
     */
    public void dropPartitionsIteratively(String dbName, String tableName,
            Map<String, String> partitionSpec, boolean ifExists, boolean deleteData)
        throws MetastoreException, TException {
        log.info("Dropping partitions iteratively.");
        List<Partition> partitions = client.listPartitionsByFilter(dbName, tableName,
                MetastoreRequestParamGenerator.generateCompositePartitionFilter(partitionSpec), (short) -1);
        for (Partition partition : partitions) {
            dropPartition(partition, ifExists, deleteData);
        }
    }

    /**
     * Copy from https://github.com/apache/hive/blob/master/hcatalog/webhcat/java-client/src/main/java/org/apache/hive/hcatalog/api/HCatClientHMSImpl.java
     */
    private void dropPartition(Partition partition, boolean ifExists, boolean deleteData)
        throws MetastoreException, MetaException, TException {
        try {
            client.dropPartition(partition.getDbName(), partition.getTableName(), partition.getValues(), deleteData);
        } catch (NoSuchObjectException e) {
            if (!ifExists) {
                throw new MetastoreException("NoSuchObjectException while dropping partition: " + partition.getValues(), e);
            }
        }
    }
}
