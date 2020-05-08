package moonset.metastore.sync.tools;

import moonset.metastore.sync.MetastoreClientFactory;
import moonset.metastore.sync.MetastoreSyncUtils;
import moonset.metastore.sync.exception.MetastoreException;
import moonset.metastore.sync.parser.ExtendedGnuParser;
import moonset.metastore.sync.util.CLIArgsTokenizer;

import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

/*
 * The command line interface of metastore sync process.
 *
 * commandline --source [hive|datacatlog] --database foo --table foo [--assume_role foo] 
 *     [--partition foo] [--all_partitions] [--replace_dest_table] [--allow_none_source_table]
 * Here are some examples.
 *
 * To sync table with some partitions:
 * commandline --source [hive|datacatlog] --database foo --table foo --partition "region_id=foo;marketplace_id=foo;snapshot_date=foo1,foo2,foo3"
 * To sync table with no partition:
 * commandline --source [hive|datacatlog] --database foo --table foo
 * To sync table with all partitions:
 * commandline --source [hive|datacatlog] --database foo --table foo --all_partitions
 * To sync table with all partitions:
 * commandline --source [hive|datacatlog] --database foo --table foo --all_partitions
 * To sync table across account with assume role.
 * commandline --source [hive|datacatlog] --database foo --table foo --assume_role  arn:aws:iam::542947461172:role/foo-role
 *
 * Now, we only support provide snapshot_date explicitly, and we plan to support start_date and end_date if we find we need that feature.
 */
public class DataCatalogSyncTool {
    private static final Log log =
        LogFactory.getLog(DataCatalogSyncTool.class);

    // Additional options for table rename.
    private static final String REMOTE_DATABASE = "remote_database";
    private static final String REMOTE_TABLE = "remote_table";

    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final String PARTITION = "partition";
    private static final String ASSUME_ROLE = "assume_role";
    private static final String ALL_PARTITIONS = "all_partitions";
    private static final String REPLACE_DEST_TABLE= "replace_dest_table";
    private static final String ALLOW_NONE_SOURCE_TABLE= "allow_none_source_table";
    private static final String SOURCE = "source";
    private static final String HIVE = "hive";
    private static final String DATACATALOG = "datacatalog";
    private static final String GLUE_REGION = "glue_region";
    private static final String DEFAULT_GLUE_REGION = "us-east-1";

    private static CommandLine parse(final String[] args) throws ParseException {
        Options options = new Options();

        Option remoteDatabase =
                OptionBuilder.withArgName("remote_database_name")
                        .hasArg()
                        .isRequired(false)
                        .withDescription("the remote database on data catalog, local database name will be used if not specified")
                        .create(REMOTE_DATABASE);
        Option remoteTable =
                OptionBuilder.withArgName("remote_table_name")
                        .hasArg()
                        .isRequired(false)
                        .withDescription("the remote table name on data catalog, local table name will be used if not specified")
                        .create(REMOTE_TABLE);

        Option localDatabase =
                OptionBuilder.withArgName("database_name")
                        .hasArg()
                        .isRequired()
                        .withDescription("local database name")
                        .create(DATABASE);
        Option destTable =
                OptionBuilder.withArgName("table_name")
                        .hasArg()
                        .isRequired()
                        .withDescription("local table name")
                        .create(TABLE);
        Option assumeRole =
                OptionBuilder.withArgName("assume_role")
                        .hasArg()
                        .isRequired(false)
                        .withDescription("cross account with assume role")
                        .create(ASSUME_ROLE);
        //Partition is optional parameter
        Option partition =
                OptionBuilder.withArgName("partition")
                        .hasArg()
                        .isRequired(false)
                        .withDescription("partition of the table to sync")
                        .create(PARTITION);

        //all_partitions is optional parameter
        Option allPartitions =
                OptionBuilder.withArgName("all_partitions")
                        .hasArg(false)
                        .isRequired(false)
                        .withDescription("all partitions of the table to sync")
                        .create(ALL_PARTITIONS);
        //replace-dest-table is optional parameter, it means to drop the existing local table, and then sync the remote table.
        Option replaceDestTable =
                OptionBuilder.withArgName("replace-dest-table") // Keep parameter key unchanged for backward compatible.
                        .hasArg(false)
                        .isRequired(false)
                        .withDescription("drop the existing local table, and then sync the remote table")
                        .create(REPLACE_DEST_TABLE);
        //allow_none_source_table is optional parameter, it means to treat it as valid if remote table does not exist.
        Option allowNoneSourceTable =
                OptionBuilder.withArgName("allow_none_source_table") // Keep parameter key unchanged for backward compatible.
                        .hasArg(false)
                        .isRequired(false)
                        .withDescription("treat it as valid if remote table does not exist")
                        .create(ALLOW_NONE_SOURCE_TABLE);
        Option source =
                OptionBuilder.withArgName("source")
                        .hasArg()
                        .isRequired()
                        .withDescription(
                                "the source metastore to sync from (indicates the direction of sync HIVE: from local to remote; DATACATLOG: from remote to local), which can be only"
                                        + HIVE
                                        + " or "
                                        + DATACATALOG)
                        .create(SOURCE);
        Option glueRegion =
                OptionBuilder.withArgName("glue_region")
                        .hasArg()
                        .isRequired(false)
                        .withDescription("The glue data catalog region")
                        .create(GLUE_REGION);

        options.addOption(localDatabase);
        options.addOption(destTable);
        options.addOption(remoteDatabase);
        options.addOption(remoteTable);
        options.addOption(assumeRole);
        options.addOption(partition);
        options.addOption(source);
        options.addOption(allPartitions);
        options.addOption(replaceDestTable);
        options.addOption(allowNoneSourceTable);
        options.addOption(glueRegion);

        CommandLineParser parser = new ExtendedGnuParser(true);
        CommandLine line = parser.parse(options, args);
        if (!HIVE.equals(line.getOptionValue(SOURCE))
                && !DATACATALOG.equals(line.getOptionValue(SOURCE))) {
            throw new ParseException(
                    "The " + SOURCE + " should be only " + HIVE + " or " + DATACATALOG);
        }
        if (line.hasOption(PARTITION) && line.hasOption(ALL_PARTITIONS)) {
            throw new ParseException(
                    "The " + PARTITION + " and " + ALL_PARTITIONS + " options should not coexist");
        }
        return line;
    }

    public static void main(final String[] args) throws Exception {
        long startTime = System.nanoTime();
        CommandLine line = parse(args);

        boolean includePartition = line.hasOption(PARTITION);
        boolean includeAllPartition = line.hasOption(ALL_PARTITIONS);
        boolean isReplaceDestTable = line.hasOption(REPLACE_DEST_TABLE);
        boolean isAllowNoneSourceTable = line.hasOption(ALLOW_NONE_SOURCE_TABLE);
        boolean isCrossAccount = line.hasOption(ASSUME_ROLE);
        String localDatabaseName = line.getOptionValue(DATABASE);
        String localTableName = line.getOptionValue(TABLE);
        String remoteDatabaseName = line.getOptionValue(REMOTE_DATABASE) == null ? localDatabaseName : line.getOptionValue(REMOTE_DATABASE);
        String remoteTableName = line.getOptionValue(REMOTE_TABLE) == null ? localTableName : line.getOptionValue(REMOTE_TABLE);


        log.info(String.format("Begin to sync table from %s. Local(Hive) : %s.%s. Remote(Data Catalog) : %s.%s. Partitions: %s.",
                line.getOptionValue(SOURCE),
                localDatabaseName,
                localTableName,
                remoteDatabaseName,
                remoteTableName,
                line.getOptionValue(PARTITION)));

        IMetaStoreClient source, dest;
        MetastoreClientFactory factory = new MetastoreClientFactory();

        String region = line.getOptionValue(GLUE_REGION) == null ? DEFAULT_GLUE_REGION : line.getOptionValue(GLUE_REGION);;

        String srcDatabaseName;
        String srcTableName;
        String destDatabaseName;
        String destTableName;

        if (DATACATALOG.equals(line.getOptionValue(SOURCE))) {
            srcDatabaseName = remoteDatabaseName;
            srcTableName = remoteTableName;
            destDatabaseName = localDatabaseName;
            destTableName = localTableName;
            if(isCrossAccount) {
                log.info("Sync across account.");
                source = factory.getDataCatalogClient(region, line.getOptionValue(ASSUME_ROLE));
            } else {
                log.info("Sync in the same account.");
                source = factory.getDataCatalogClient(region);
            }
            dest = factory.getHiveMetastoreClient(MetastoreClientFactory.EMR_HIVE_SITE_XML_PATH);
        } else {
            srcDatabaseName = localDatabaseName;
            srcTableName = localTableName;
            destDatabaseName = remoteDatabaseName;
            destTableName = remoteTableName;
            if(isCrossAccount) {
                log.info("Sync across account.");
                dest = factory.getDataCatalogClient(region, line.getOptionValue(ASSUME_ROLE));
            } else {
                log.info("Sync in the same account.");
                dest = factory.getDataCatalogClient(region);
            }
            source = factory.getHiveMetastoreClient(MetastoreClientFactory.EMR_HIVE_SITE_XML_PATH);
        }

        if(isReplaceDestTable && dest.tableExists(destDatabaseName, destTableName)){
            log.info("Replace mode, drop the dest table and then sync source table.");
            dest.dropTable(destDatabaseName, destTableName);
        }
        if(!source.tableExists(srcDatabaseName, srcTableName)){
            if (!isAllowNoneSourceTable) {
                throw new MetastoreException("The " + srcDatabaseName + "." + srcTableName + " not exists.");
            }else{
                //do nothing since the source table do not exist, since isAllowNoneSourceTable is set, this case is valid.
                log.info("The source table does not exist, since isAllowNoneSourceTable is set, this case is valid.");
            }
        } else {
            log.trace("Begin to sync table.");
            MetastoreSyncUtils.syncTable(source, dest, srcDatabaseName, srcTableName, destDatabaseName, destTableName);
            if (includePartition) {
                log.trace(String.format("Begin to sync partitionsi %s.", line.getOptionValue(PARTITION)));
                MetastoreSyncUtils.syncPartitions(source, dest, srcDatabaseName, srcTableName, destDatabaseName, destTableName, CLIArgsTokenizer.parsePartition(line.getOptionValue(PARTITION)));
            }
            if (includeAllPartition) {
                log.trace("Begin to sync all partitions.");
                MetastoreSyncUtils.syncAllPartitions(source, dest, srcDatabaseName, srcTableName, destDatabaseName, destTableName);
            }
        }
        log.info("Sync successfully");
        source.close();
        dest.close();

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);

        log.info(
                "The sync task took "
                        + String.format(
                                "%d hour, %d min, %d sec",
                                TimeUnit.NANOSECONDS.toHours(duration),
                                TimeUnit.NANOSECONDS.toMinutes(duration) % 60,
                                TimeUnit.NANOSECONDS.toSeconds(duration) % 60));
    }
}
