# MoonsetMetastoreSync

To sync between Glue data catalog and EMR's local metastore. The command runs
inside AWS EMR. The following is a sample execution.

```bash
[hadoop@ip-10-0-168-123 ~]$ curl -LO https://github.com/FBAChinaOpenSource/MoonsetMetastoreSync/releases/download/v0.0.1/MoonsetMetaStoreSync.jar
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   615  100   615    0     0   5082      0 --:--:-- --:--:-- --:--:--  5082
100 31423  100 31423    0     0   166k      0 --:--:-- --:--:-- --:--:--  166k

[hadoop@ip-10-0-168-123 ~]$ java -cp MoonsetMetaStoreSync.jar:/usr/lib/hive/lib/*:/usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hive/auxlib/*:/usr/share/aws/aws-java-sdk/* moonset.metastore.sync.tools.DataCatalogSyncTool --database zhihaow --table foo --source datacatalog --all_partitions
2020-03-19T00:35:11,694 INFO [main] moonset.metastore.sync.tools.DataCatalogSyncTool - Begin to sync table from datacatalog. Local(Hive) : zhihaow.foo. Remote(Data Catalog) : zhihaow.foo. Partitions: null.
2020-03-19T00:35:11,719 INFO [main] org.apache.hadoop.hive.conf.HiveConf - Found configuration file null

2020-03-19T00:35:12,223 INFO [main] com.amazonaws.glue.catalog.metastore.AWSGlueClientFactory - Setting region to : us-east-1
2020-03-19T00:35:13,023 WARN [main] com.amazonaws.profile.path.cred.CredentialsLegacyConfigLocationProvider - Found the legacy config profiles file at [/home/hadoop/.aws/config]. Please move it to the latest default location [~/.aws/credentials].
2020-03-19T00:35:13,468 INFO [main] hive.metastore - Trying to connect to metastore with URI thrift://ip-10-0-168-123.ec2.internal:9083
2020-03-19T00:35:13,483 INFO [main] hive.metastore - Opened a connection to metastore, current connections: 1
2020-03-19T00:35:13,543 WARN [main] org.apache.hadoop.util.NativeCodeLoader - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2020-03-19T00:35:13,597 INFO [main] hive.metastore - Connected to metastore.
2020-03-19T00:35:13,928 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - The database zhihaow already exists in destination metastore, and we need not sync it again.
2020-03-19T00:35:13,948 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - The table foo already exists in destination metastore, and we only need to sync table properties.
2020-03-19T00:35:13,958 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - The table original properties: {transient_lastDdlTime=1514369509, EXTERNAL=TRUE, original_create_time=0}
2020-03-19T00:35:14,105 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - The table properties after updated: {transient_lastDdlTime=1514369509, EXTERNAL=TRUE, original_create_time=0}
2020-03-19T00:35:14,152 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - The table properties updated successfully.
2020-03-19T00:35:25,950 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - There are 1 partitions needed to sync.
2020-03-19T00:35:25,950 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - Rewrite the partition location to directory if any file location exists.
2020-03-19T00:35:25,957 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - Rewrite complete.
2020-03-19T00:35:25,957 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - Update database name and table name to dest database and dest table.
2020-03-19T00:35:25,957 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - Database name and table name updated.
2020-03-19T00:35:25,957 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - Add original create time to partitions.
2020-03-19T00:35:25,958 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - Original create time added.
2020-03-19T00:35:25,958 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - Begin to sync partitions.
2020-03-19T00:35:25,971 INFO [main] moonset.metastore.sync.MetastoreSyncUtils - The range [ 0, 1 ) partitions have been synced.
2020-03-19T00:35:25,971 INFO [main] moonset.metastore.sync.tools.DataCatalogSyncTool - Sync successfully
2020-03-19T00:35:25,973 INFO [main] hive.metastore - Closed a connection to metastore, current connections: 0
2020-03-19T00:35:25,973 INFO [main] moonset.metastore.sync.tools.DataCatalogSyncTool - The sync task took 0 hour, 0 min, 14 sec
```
