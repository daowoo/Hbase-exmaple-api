package com.example.hbase.admin;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;

import java.util.BitSet;

/** 
* NameSpace Tester. 
* 
* @author <Authors name> 
* @since <pre> </pre>
* @version 1.0 
*/
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NameSpaceTest extends BaseTest {
    protected static final String NAMESPACE_NAME = "daowoo";

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeclass() throws Exception {
        configuration();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName[] tables = admin.listTableNamesByNamespace(NAMESPACE_NAME);
            for (TableName table : tables) {
                admin.disableTable(table);
                admin.deleteTable(table);
            }

            admin.deleteNamespace(NAMESPACE_NAME);
        }
        catch (NamespaceNotFoundException e) {
            System.out.printf("namespace %s not found\n", NAMESPACE_NAME);
        }
    }

    @Test
    public void test001CreateNs() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {


            NamespaceDescriptor ns = NamespaceDescriptor.create(NAMESPACE_NAME).build();
            admin.createNamespace(ns);

            NamespaceDescriptor rns = admin.getNamespaceDescriptor(NAMESPACE_NAME);
            Assert.assertNotNull(rns);
        }
    }

    @Test
    public void test002ModifyNs() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            NamespaceDescriptor ns = admin.getNamespaceDescriptor(NAMESPACE_NAME);

            String k1 = "myname";
            String v1 = "phf";
            ns.setConfiguration(k1,v1);

            String k2 = "hbase.namespace.quota.maxtables";
            int v2 = 5;
            ns.setConfiguration(k2,String.valueOf(v2));

            admin.modifyNamespace(ns);

            NamespaceDescriptor rns = admin.getNamespaceDescriptor(NAMESPACE_NAME);
            String rv1 = rns.getConfigurationValue(k1);
            Assert.assertEquals(v1,rv1);

            int rv2 = Integer.valueOf(rns.getConfigurationValue(k2)).intValue();
            Assert.assertEquals(v2,rv2);
        }
    }

    @Test
    public void test003ListNs() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            NamespaceDescriptor[] list_ns = admin.listNamespaceDescriptors();

            BitSet flag = new BitSet();
            for (NamespaceDescriptor ns : list_ns) {
                if (ns.getName().equals("hbase")) flag.set(0);
                else if (ns.getName().equals("default")) flag.set(1);
                else if (ns.getName().equals(NAMESPACE_NAME)) flag.set(2);
            }

            Assert.assertEquals(3,flag.cardinality());
        }
    }


    @Test
    public void test004DropNs() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            admin.deleteNamespace(NAMESPACE_NAME);

            expectedEx.expect(NamespaceNotFoundException.class);
            NamespaceDescriptor rns = admin.getNamespaceDescriptor(NAMESPACE_NAME);
            Assert.assertNull(rns);
        }
    }
}
