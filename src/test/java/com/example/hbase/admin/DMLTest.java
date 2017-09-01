package com.example.hbase.admin;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * NameSpace Tester.
 *
 * @author <Authors name>
 * @since <pre> </pre>
 * @version 1.0
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DMLTest extends BaseTest {
    protected static final String NAMESPACE_NAME = "daowoo";
    protected static final String TABLE_NAME = "test2";
    protected static final String CF_DEFAULT = "f1";
    protected static final String COL_NAME = "name";
    protected static final String COL_AGE = "age";
    protected static final String COL_DESC = "desc";
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
                try {
                    NamespaceDescriptor ns = NamespaceDescriptor.create(NAMESPACE_NAME).build();
                    admin.createNamespace(ns);
                }
                catch (NamespaceExistException e) {
                    System.out.printf("namespace %s is exists\n", NAMESPACE_NAME);
                }

            }

            HTableDescriptor table = new HTableDescriptor(tablename);
            HColumnDescriptor newColumn = new HColumnDescriptor(CF_DEFAULT);
            newColumn.setMaxVersions(CELL_VERSION);
            table.addFamily(newColumn);

            admin.createTable(table);
            Assert.assertTrue(admin.tableExists(table.getTableName()));
        }
    }

    @Test
    public void test001Put() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME)))) {

            int rowNum = 500;

            for (int r = 0; r < rowNum; r++) {
                String rowKey = buildRowKey(r+1);

                Put p = new Put(Bytes.toBytes(rowKey));

                for (int i = 0; i < CELL_VERSION; i++) {
                    String name = String.format("TEST-%08d", i);
                    p.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_NAME), i + 1, Bytes.toBytes(name));

                    String age = String.valueOf(10 + i);
                    p.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_AGE), i + 1, Bytes.toBytes(age));

                    String desc = String.format("ID:%08d", i);
                    p.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_DESC), i + 1, Bytes.toBytes(desc));
                }

                tt.put(p);
            }
        }
    }

    @Test
    public void test002Get() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME)))) {

            int row = 100;
            String rowKey = buildRowKey(row);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setMaxVersions(CELL_VERSION);

            Result result = tt.get(get);
        }
    }

    @Test
    public void test003Scan() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(buildTableName(NAMESPACE_NAME, TABLE_NAME)))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(1);

            String start_key = buildRowKey(50);
            String end_key = buildRowKey(100);

            s.setStartRow(Bytes.toBytes(start_key));
            s.setStopRow(Bytes.toBytes(end_key));

            ResultScanner scanner = tt.getScanner(s);
            for (Result result : scanner) {

            }
            scanner.close();
        }
    }

    @Test
    public void test004Apend() throws Exception {

    }

    @Test
    public void test005Batch() throws Exception {

    }
}
