package com.example.hbase.admin;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.runners.MethodSorters;

//指定测试方法按照定义的顺序执行
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TableTest extends BaseTest {

    @Test
    public void test001CreateTable() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            HColumnDescriptor newColumn = new HColumnDescriptor(CF_DEFAULT);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.addFamily(newColumn);

            System.out.printf("Creating table %s", table.getTableName());
            createOrOverwrite(admin, table);
            System.out.println(" Done.");
        }
    }

    @Test
    public void test002PropertyTable() throws Exception {

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        String table_name = table.getNameAsString();
        Assert.assertNotNull(table_name);

        System.out.printf("Read property from table %s", table.getTableName());

        long file_size = table.getMaxFileSize();
        Assert.assertNotEquals(file_size, 0);

        boolean read_only = table.isReadOnly();
        Assert.assertFalse(read_only);

        long flush_size = table.getMemStoreFlushSize();
        Assert.assertNotEquals(flush_size, 0);

        System.out.println(" Done.");
    }

    @Test
    public void test003PutCell() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            int rowNum = 300;
            int verNum = 20;

            for (int r = 0; r < rowNum; r++) {
                String rowKey = buildRowKey(r+1);

                Put p = new Put(Bytes.toBytes(rowKey));
                System.out.printf("Put cell into table = %s, row = %s", TABLE_NAME, rowKey);

                for (int i = 0; i < verNum; i++) {
                    String name = String.format("daowoo-%08d", i);
                    p.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_NAME), i + 1, Bytes.toBytes(name));

                    String age = String.valueOf(10 + i);
                    p.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_AGE), i + 1, Bytes.toBytes(age));

                    String desc = String.format("markdown is %08d", i);
                    p.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_DESC), i + 1, Bytes.toBytes(desc));
                }

                tt.put(p);
                System.out.println(" Done.");
            }
        }
    }

    @Test
    public void test004DisableTable() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                Assert.fail(String.format("Table %s does not exist.", TABLE_NAME));
            }

            System.out.printf("Disable table %s", tableName.getNameAsString());
            admin.disableTable(tableName);
            System.out.println(" Done.");
        }
    }

    @Test
    public void test005AddColFamily() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                Assert.fail(String.format("Table %s does not exist.", TABLE_NAME));
            }

            String cf_f2 = "f2";
            System.out.printf("Add new column %s into table %s", cf_f2, TABLE_NAME);
            HColumnDescriptor col_f2 = new HColumnDescriptor(cf_f2);
            col_f2.setCompactionCompressionType(Compression.Algorithm.GZ);
            col_f2.setMaxVersions(CELL_VERSION);
            admin.addColumn(tableName, col_f2);
            System.out.println(" Done.");

            String cf_f3 = "f3";
            System.out.printf("Add new column %s into table %s", cf_f3, TABLE_NAME);
            HColumnDescriptor col_f3 = new HColumnDescriptor(cf_f3);
            col_f3.setCompactionCompressionType(Compression.Algorithm.GZ);
            col_f3.setMaxVersions(CELL_VERSION);
            admin.addColumn(tableName, col_f3);
            System.out.println(" Done.");
        }
    }

    @Test
    public void test006ModifyTable() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                Assert.fail(String.format("Table %s does not exist.", TABLE_NAME));
            }

            System.out.printf("Modify column %s to table %s", CF_DEFAULT, TABLE_NAME);
            HTableDescriptor table = admin.getTableDescriptor(tableName);
            HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
            existingColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            existingColumn.setMaxVersions(CELL_VERSION);
            table.modifyFamily(existingColumn);
            admin.modifyTable(tableName, table);
            System.out.println(" Done.");
        }
    }

    @Test
    public void test007EnableTable() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                Assert.fail(String.format("Table %s does not exist.", TABLE_NAME));
            }

            System.out.printf("Enable table %s", tableName.getNameAsString());
            admin.enableTable(tableName);
            System.out.println(" Done.");
        }
    }

    @Test
    public void test008GetCell() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            int row = 10;
            String rowKey = buildRowKey(row);
            Get get1 = new Get(Bytes.toBytes(rowKey));
            get1.setMaxVersions(CELL_VERSION);

            System.out.printf("get Cell from table = %s \n", TABLE_NAME);
            Result result = tt.get(get1);
            printResult(result,CF_DEFAULT,COL_DESC);
        }
    }

    @Test
    public void test009ScanCell() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();
            System.out.printf("Scan data from table = %s \n", TABLE_NAME);

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(3);

            String start_key = buildRowKey(50);
            String end_key = buildRowKey(100);

            s.setStartRow(Bytes.toBytes(start_key));
            s.setStopRow(Bytes.toBytes(end_key));

            ResultScanner scanner = tt.getScanner(s);
            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_NAME);
            }
            scanner.close();
            System.out.println(" Done.");
        }
    }
}

