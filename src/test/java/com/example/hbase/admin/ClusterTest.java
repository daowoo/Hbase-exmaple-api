package com.example.hbase.admin;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;

import static org.hamcrest.CoreMatchers.*;

/**
 * NameSpace Tester.
 *
 * @author <Authors name>
 * @since <pre> </pre>
 * @version 1.0
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ClusterTest extends BaseTest {
    protected static final String HBASE_VERSION = "1.1.2.2.6.1.0-129";
    protected static final String MASTER_FQDN = "nn.daowoo.com";
    protected static final int LIVE_NUMBER = 3;
    protected static final int DEAD_MUMBER = 0;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void test001ClusterVersion() throws Exception {
        configuration();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            ClusterStatus cluster = admin.getClusterStatus();

            String version = cluster.getHBaseVersion();
            Assert.assertEquals(HBASE_VERSION,version);
        }
    }

    @Test
    public void test002ClusterMaster() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            ClusterStatus cluster = admin.getClusterStatus();

            String master_host = cluster.getMaster().getHostname();
            Assert.assertEquals(MASTER_FQDN,master_host);

            String master_server = cluster.getMaster().getServerName();
            Assert.assertThat(master_server,containsString(MASTER_FQDN));
        }
    }

    @Test
    public void test003ClusterServer() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            ClusterStatus cluster = admin.getClusterStatus();

            int live_num = cluster.getServers().size();
            Assert.assertEquals(LIVE_NUMBER,live_num);

            int dead_num = cluster.getDeadServers();
            Assert.assertEquals(DEAD_MUMBER,dead_num);
        }
    }
}
