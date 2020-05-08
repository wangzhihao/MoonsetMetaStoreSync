package moonset.metastore.sync;

import moonset.metastore.sync.exception.MetastoreException;
import com.amazonaws.glue.catalog.metastore.AWSGlueClientFactory;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.concurrent.TimeUnit;
/**
 * This class is a factory to get IMetastoreClient isntances in different ways. It reads common
 * configuration from brazil config.
 */
public class MetastoreClientFactory {
    public static final String AWS_GLUE_REGION = "aws.glue.region";
    public static final String EMR_HIVE_SITE_XML_PATH = "file:///etc/hive/conf/hive-site.xml";
    private final static int HIVE_METASTORE_PORT = 9083;

    /**
     * Constructor.
     */
    public MetastoreClientFactory() {
    }

    /**
     * To access the metastore on an EMR cluster over a ssh tunnel.
     */
    public IMetaStoreClient getMetastoreClient(Session session) throws MetastoreException {
        try {
            //0 means to bind an available port.
            int port = session.setPortForwardingL(0, session.getHost(), HIVE_METASTORE_PORT);
            return getThriftMetastoreClient("localhost", port);
        } catch (JSchException e) {
            throw new MetastoreException("can't new a metastoreclient.", e);
        }
    }

    /**
     * To access the metastore via thrift. This method requires host is public accessible,
     * which usually isn't the case for safty concern.
     */
    public IMetaStoreClient getThriftMetastoreClient(String host, int port) throws MetastoreException {
        try {
            HiveConf conf = new HiveConf();
            conf.setVar(HiveConf.ConfVars.METASTOREURIS, String.format("thrift://%s:%d", host, port));
            return new HiveMetaStoreClient(conf);
        } catch (MetaException e) {
            throw new MetastoreException("can't new a metastoreclient.", e);
        }
    }

    /**
     * Get a hivemetastore client on EMR directly, so we don't need to worry about the crendentials.
     *
     * @param hiveSiteXmlPath the path to hive-site.xml on EMR.
     * @return a IMetastoreClient istance for hive metastore on EMR.
     * @throws MetastoreException if failed to get an IMetaStoreClient istance.
     */
    public IMetaStoreClient getHiveMetastoreClient(String hiveSiteXmlPath) throws MetastoreException {
        try {
            HiveConf conf = new HiveConf();
            conf.addResource(new Path(hiveSiteXmlPath));
            conf.setTimeVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 6000, TimeUnit.SECONDS);
            return new HiveMetaStoreClient(conf);
        } catch (MetaException e) {
            throw new MetastoreException("can't new a metastoreclient.", e);
        }
    }

    /**
     * Get a datacatalog client on EMR directly, so we don't need to worry about the crendentials.
     *
     * @param region the glue service region.
     * @return a IMetaStoreClient instance for AWS DataCatalog on EMR.
     * @throws MetastoreException if failed to get an IMetaStoreClient istance.
     */
    public IMetaStoreClient getDataCatalogClient(String region) throws MetastoreException {
        try {
            HiveConf conf = new HiveConf();
            conf.set(AWSGlueClientFactory.AWS_REGION, region);
            return new NoFileSystemOpsAWSCatalogMetastoreClient(conf);
        } catch (MetaException e) {
            throw new MetastoreException("can't new a metastoreclient.", e);
        }
    }

    /**
     * Get a datacatlog client across account. 
     *
     * @param region the glue service region.
     * @param assumeRole the role to assume in the target account. 
     * @return an IMetaStoreClient instance for AWS DataCatalog not on EMR.
     * @throws MetastoreException if failed to get an IMetaStoreClient istance.
     */
    public IMetaStoreClient getDataCatalogClient(final String region, final String assumeRole)
            throws MetastoreException {
        try {
            HiveConf conf = new HiveConf();
            conf.set(AWSGlueClientFactory.AWS_REGION, region);
            conf.set(AWSGlueClientFactory.AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS,
                    "moonset.metastore.sync.RoleBasedAWSCredentialsProviderFactory");
            conf.set(RoleBasedAWSCredentialsProviderFactory.ASSUME_ROLE, assumeRole);

            return new NoFileSystemOpsAWSCatalogMetastoreClient(conf);
        } catch (MetaException e) {
            throw new MetastoreException("can't new a metastoreclient.", e);
        }
    }
}
