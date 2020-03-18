package moonset.metastore.sync;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.GlueInputConverter;
import com.amazonaws.glue.catalog.converters.HiveToCatalogConverter;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.amazonaws.glue.catalog.util.BatchCreatePartitionsHelper;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.thrift.TException;

/**
 * Wrapper of AWSCatalogMetastoreClient. Only comment out the file system(warehouse) operators since
 * we have already done them in local hive metastore. No need to check it again. What's more, to do
 * file system operations is nearly impossible, which is similar to start a hadoop cluster by our
 * own hand. 
 *
 * The code of AWSCatalogMetastoreClient is in this repo:
 * https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore/blob/master/aws-glue-datacatalog-hive2-client/src/main/java/com/amazonaws/glue/catalog/metastore/AWSCatalogMetastoreClient.java
 */
@Slf4j
public class NoFileSystemOpsAWSCatalogMetastoreClient extends AWSCatalogMetastoreClient {

    public NoFileSystemOpsAWSCatalogMetastoreClient(HiveConf conf) throws MetaException {
        super(conf);
    }

    private static final int BATCH_CREATE_PARTITIONS_PAGE_SIZE = 100;
    private static final int BATCH_CREATE_PARTITIONS_THREADS_COUNT = 5;
    private static final ExecutorService BATCH_CREATE_PARTITIONS_THREAD_POOL =
            Executors.newFixedThreadPool(BATCH_CREATE_PARTITIONS_THREADS_COUNT);

    @Override
    public void createDatabase(org.apache.hadoop.hive.metastore.api.Database database)
            throws InvalidObjectException,
                    org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
                    TException {
                    throw new MetaException("Prohibit to do create database back to aws data catalog, Only allow to add partitions.");
    }

    @Override
    public void createTable(org.apache.hadoop.hive.metastore.api.Table tbl) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, InvalidObjectException, MetaException,
           NoSuchObjectException, TException {
               try {
                   TableInput tableInput = GlueInputConverter.convertToTableInput(tbl);
                   // TODO Hive metastore does this on the server side, but for now we do it here
                   // since we have access to the DDL_TIME constant.
                   if (tableInput.getParameters() == null) {
                       tableInput.setParameters(Maps.<String, String>newHashMap());
                   }
                   long time = System.currentTimeMillis() / 1000;
                   tableInput.getParameters().put(hive_metastoreConstants.DDL_TIME, Long.toString(time));

                   getClient().createTable(new CreateTableRequest().withTableInput(tableInput).withDatabaseName(tbl.getDbName()));
               } catch (AmazonServiceException e){
                   throw CatalogToHiveConverter.wrapInHiveException(e);
               } catch (Exception e){
                   String msg = "Unable to create table: ";
                   throw new MetaException(msg + e);
               }
    }

    /**
     * Only used to update table parameters.
     */
    @Override
    public void alter_table(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Table table)
    throws InvalidOperationException, MetaException, TException {
        try{
            TableInput newTableInput = GlueInputConverter.convertToTableInput(table);
            getClient().updateTable(new UpdateTableRequest().withDatabaseName(dbName).withTableInput(newTableInput));
        }catch(Exception e){
            throw new TException("cannot update table in glue.", e);
        }
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> add_partitions(
            List<org.apache.hadoop.hive.metastore.api.Partition> partitions,
            boolean ifNotExists,
            boolean needResult)
            throws TException {

        List<Partition> partitionsCreated = batchCreatePartitions(partitions, ifNotExists);
        if (!needResult) {
            return null;
        }
        return CatalogToHiveConverter.convertPartitions(partitionsCreated);
    }

    private List<Partition> batchCreatePartitions(
            final List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions,
            final boolean ifNotExists)
            throws TException {

        if (hivePartitions == null || hivePartitions.isEmpty()) {
            return Lists.newArrayList();
        }

        List<Partition> catalogPartitions = Lists.newArrayList();
        for (org.apache.hadoop.hive.metastore.api.Partition partition : hivePartitions) {
            Partition catalogPartition = HiveToCatalogConverter.convertPartition(partition);
            catalogPartitions.add(catalogPartition);
        }

        org.apache.hadoop.hive.metastore.api.Partition firstPartition = hivePartitions.get(0);
        final String namespaceName = firstPartition.getDbName();
        final String tableName = firstPartition.getTableName();

        List<Future<BatchCreatePartitionsHelper>> batchCreatePartitionsFutures =
                Lists.newArrayList();
        for (int i = 0; i < catalogPartitions.size(); i += BATCH_CREATE_PARTITIONS_PAGE_SIZE) {
            int j = Math.min(i + BATCH_CREATE_PARTITIONS_PAGE_SIZE, catalogPartitions.size());
            final List<Partition> partitionsOnePage = catalogPartitions.subList(i, j);

            batchCreatePartitionsFutures.add(
                    BATCH_CREATE_PARTITIONS_THREAD_POOL.submit(
                            new Callable<BatchCreatePartitionsHelper>() {
                                @Override
                                public BatchCreatePartitionsHelper call() throws Exception {
                                    return new BatchCreatePartitionsHelper(
                                                    getClient(),
                                                    namespaceName,
                                                    tableName,
                                                    partitionsOnePage,
                                                    ifNotExists)
                                            .createPartitions();
                                }
                            }));
        }

        TException tException = null;
        List<Partition> partitionsCreated = Lists.newArrayList();
        for (Future<BatchCreatePartitionsHelper> future : batchCreatePartitionsFutures) {
            try {
                BatchCreatePartitionsHelper batchCreatePartitionsHelper = future.get();
                partitionsCreated.addAll(batchCreatePartitionsHelper.getPartitionsCreated());
                tException =
                        tException == null
                                ? batchCreatePartitionsHelper.getFirstTException()
                                : tException;
            } catch (Exception e) {
                log.error("Exception thrown by BatchCreatePartitions thread pool. ", e);
            }
        }

        if (tException != null) {
            throw tException;
        }
        return partitionsCreated;
    }

    @Override
    public void close() {
        super.close();
        try {
            if (null != BATCH_CREATE_PARTITIONS_THREAD_POOL) {
                BATCH_CREATE_PARTITIONS_THREAD_POOL.shutdownNow();
            }
        } catch (SecurityException e) {
            log.error("Unable to shutdown metastore client.", e);
        }
    }
    /** Get super class's hidden field . */
    private AWSGlue getClient() throws Exception {
        Field client = AWSCatalogMetastoreClient.class.getDeclaredField("glueClient");
        client.setAccessible(true);
        return (AWSGlue) client.get(this);
    }
}
