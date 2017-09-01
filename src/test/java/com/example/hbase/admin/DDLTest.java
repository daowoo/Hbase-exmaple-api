package com.example.hbase.admin;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Set;


/**
 * NameSpace Tester.
 *
 * @author <Authors name>
 * @since <pre> </pre>
 * @version 1.0
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DDLTest extends BaseTest {
    protected static final String NAMESPACE_NAME = "daowoo";
    protected static final String TABLE_NAME = "test1";
    protected static final String CF_DEFAULT = "f1";
    protected static final int    CELL_VERSION = 5;

    @BeforeClass
    public static void beforeclass() throws Exception {
        configuration();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            String table_name = buildTableName(NAMESPACE_NAME, TABLE_NAME);
            TableName tablename = TableName.valueOf(table_name);

            if (admin.tableExists(tablename)) {
                admin.disableTable(tablename);
                admin.deleteTable(tablename);
            }
            else {
                NamespaceDescriptor ns = NamespaceDescriptor.create(NAMESPACE_NAME).build();
                admin.createNamespace(ns);
            }
        }
        catch (NamespaceExistException e) {
            System.out.printf("namespace %s is exists\n", NAMESPACE_NAME);
        }
    }

    @Test
    public void test001CreateTable() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME));
            HTableDescriptor table = new HTableDescriptor(tableName);

            HColumnDescriptor newColumn = new HColumnDescriptor(CF_DEFAULT);
            table.addFamily(newColumn);

            admin.createTable(table);
            Assert.assertTrue(admin.tableExists(table.getTableName()));
        }
    }

    @Test
    public void test002ModifyTable() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME));
            HTableDescriptor table = admin.getTableDescriptor(tableName);
            Assert.assertTrue(admin.tableExists(tableName));

            HColumnDescriptor column = table.getFamily(Bytes.toBytes(CF_DEFAULT));
            column.setCompactionCompressionType(Compression.Algorithm.GZ);
            column.setMaxVersions(CELL_VERSION);
            admin.modifyColumn(tableName,column);

            column = table.getFamily(Bytes.toBytes(CF_DEFAULT));
            Assert.assertNotNull(column);
            Assert.assertEquals(CELL_VERSION, column.getMaxVersions());
            Assert.assertEquals(Compression.Algorithm.GZ, column.getCompactionCompressionType());
        }
    }

    @Test
    public void test003ConfigurationTable() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME));
            Assert.assertTrue(admin.tableExists(tableName));

            HTableDescriptor table = admin.getTableDescriptor(tableName);

            int min_files = 2;
            table.setConfiguration("hbase.hstore.compaction.min", String.valueOf(min_files));
            admin.modifyTable(tableName, table);

            table = admin.getTableDescriptor(tableName);
            String val = table.getConfigurationValue("hbase.hstore.compaction.min");
            Assert.assertEquals(min_files, Integer.valueOf(val).intValue());
        }
    }

    @Test
    public void test004PropertyTable() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME));
            Assert.assertTrue(admin.tableExists(tableName));

            HTableDescriptor table = admin.getTableDescriptor(tableName);

            String key1 = "name";
            String val1 = "phf";
            table.setValue(key1,val1);

            String key2 = "age";
            int val2 = 32;
            table.setValue(key2,String.valueOf(val2));
            admin.modifyTable(tableName, table);

            table = admin.getTableDescriptor(tableName);
            Assert.assertEquals(val1, table.getValue(key1));
            Assert.assertEquals(val2, Integer.valueOf(table.getValue(key2)).intValue());
        }
    }

    @Test
    public void test005ConfigurationFamily() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME));
            Assert.assertTrue(admin.tableExists(tableName));

            HTableDescriptor table = admin.getTableDescriptor(tableName);
            HColumnDescriptor column = table.getFamily(Bytes.toBytes(CF_DEFAULT));

            int store_files = 15;
            column.setConfiguration("hbase.hstore.blockingStoreFiles", String.valueOf(store_files));
            admin.modifyColumn(tableName,column);

            column = table.getFamily(Bytes.toBytes(CF_DEFAULT));
            String val = column.getConfigurationValue("hbase.hstore.blockingStoreFiles");
            Assert.assertEquals(store_files, Integer.valueOf(val).intValue());
        }
    }

    @Test
    public void test006PropertyFamily() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME));
            Assert.assertTrue(admin.tableExists(tableName));

            HTableDescriptor table = admin.getTableDescriptor(tableName);
            HColumnDescriptor column = table.getFamily(Bytes.toBytes(CF_DEFAULT));

            String key1 = "name";
            String val1 = "phf";
            column.setValue(key1,val1);

            String key2 = "age";
            int val2 = 32;
            column.setValue(key2,String.valueOf(val2));
            admin.modifyColumn(tableName,column);

            column = table.getFamily(Bytes.toBytes(CF_DEFAULT));
            Assert.assertEquals(val1, column.getValue(key1));
            Assert.assertEquals(val2, Integer.valueOf(column.getValue(key2)).intValue());
        }
    }

    @Test
    public void test007AddFamily() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME));
            Assert.assertTrue(admin.tableExists(tableName));

            String cf_f2 = "f2";
            HColumnDescriptor col_f2 = new HColumnDescriptor(cf_f2);
            col_f2.setCompactionCompressionType(Compression.Algorithm.GZ);
            col_f2.setMaxVersions(CELL_VERSION);
            admin.addColumn(tableName, col_f2);

            String cf_f3 = "f3";
            HColumnDescriptor col_f3 = new HColumnDescriptor(cf_f3);
            col_f3.setCompactionCompressionType(Compression.Algorithm.GZ);
            col_f3.setMaxVersions(CELL_VERSION);
            admin.addColumn(tableName, col_f3);

            HTableDescriptor table = admin.getTableDescriptor(tableName);
            Set<byte[]> cf_names = table.getFamiliesKeys();
            Assert.assertTrue(cf_names.contains(Bytes.toBytes(cf_f2)));
            Assert.assertTrue(cf_names.contains(Bytes.toBytes(cf_f3)));
        }
    }

    @Test
    public void test008DeleteFamily() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME));
            Assert.assertTrue(admin.tableExists(tableName));

            String cf_f2 = "f2";
            admin.deleteColumn(tableName, Bytes.toBytes(cf_f2));

            String cf_f3 = "f3";
            admin.deleteColumn(tableName, Bytes.toBytes(cf_f3));

            HTableDescriptor table = admin.getTableDescriptor(tableName);
            Set<byte[]> cf_names = table.getFamiliesKeys();
            Assert.assertFalse(cf_names.contains(Bytes.toBytes(cf_f2)));
            Assert.assertFalse(cf_names.contains(Bytes.toBytes(cf_f3)));
        }
    }

    @Test
    public void test009DisableTable() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME));
            Assert.assertTrue(admin.tableExists(tableName));

            admin.disableTable(tableName);
            Assert.assertTrue(admin.isTableDisabled(tableName));
        }
    }

    @Test
    public void test010EnableTable() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME));
            Assert.assertTrue(admin.tableExists(tableName));

            admin.enableTable(tableName);
            Assert.assertTrue(admin.isTableEnabled(tableName));
        }
    }

    @Test
    public void test011DeleteTable() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME));
            Assert.assertTrue(admin.tableExists(tableName));

            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            Assert.assertFalse(admin.tableExists(tableName));
        }
    }
}
