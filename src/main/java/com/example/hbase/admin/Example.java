package com.example.hbase.admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class Example {

    private static final String TABLE_NAME = "test:t2";
    private static final String CF_DEFAULT = "f1";
    private static final String COL_NAME = "name";
    private static final String COL_AGE = "age";
    private static final String COL_DESC = "desc";

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createSchemaTables(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            HColumnDescriptor newColumn = new HColumnDescriptor(CF_DEFAULT);
            newColumn.setCompactionCompressionType(Algorithm.NONE);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.addFamily(newColumn);

            System.out.printf("Creating table. %s", table.getTableName());
            createOrOverwrite(admin, table);
            System.out.println(" Done.");
        }
    }

    public static void modifySchema(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist.");
                System.exit(-1);
            }

            HTableDescriptor table = admin.getTableDescriptor(tableName);

            // Update existing table
            HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
            newColumn.setCompactionCompressionType(Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName, newColumn);

            // Update existing column family
            HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
            existingColumn.setCompactionCompressionType(Algorithm.GZ);
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(existingColumn);
            admin.modifyTable(tableName, table);

            // Disable an existing table
            admin.disableTable(tableName);

            // Delete an existing column family
            admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));

            // Delete a table (Need to be disabled first)
            admin.deleteTable(tableName);
        }
    }

    public  static void printResult(Result result, String fm, String col) {
        System.out.println(result);

        List<Cell> cells = result.getColumnCells(Bytes.toBytes(fm), Bytes.toBytes(col));
        for (Cell cell : cells) {
            String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            System.out.printf("%s:%s: %s timestamp: %d",fm,col,val,cell.getTimestamp());
            System.out.print(" | ");
        }
        System.out.println();
    }

    public static void putData(Configuration config) throws IOException {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            int rowNum = 100;
            int verNum = 20;

            for (int r = 0; r < rowNum; r++) {
                String rowKey = String.format("User%08d", r + 1);

                Put p = new Put(Bytes.toBytes(rowKey));
                System.out.printf("Put Cell into table = %s, row = %s", TABLE_NAME, rowKey);

                for (int i = 0; i < verNum; i++) {
                    String name = String.format("panhongfa%04d", i);
                    p.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_NAME), i+1, Bytes.toBytes(name));

                    String age = String.valueOf(10 + i);
                    p.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_AGE), i+1, Bytes.toBytes(age));

                    String desc = String.format("markdown format is %04d", i);
                    p.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_DESC), i+1, Bytes.toBytes(desc));
                }

                tt.put(p);
                System.out.println(" Done.");
            }
        }
    }

    public static void scanData(Configuration config) throws IOException {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();
            System.out.printf("scan Cell from table = %s \n", TABLE_NAME);

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
//            s.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_AGE));

            s.setMaxVersions(20);
            ResultScanner scanner = tt.getScanner(s);
            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_NAME);
            }
            scanner.close();
            System.out.println(" Done.");
        }
    }

    public static void scanDataWithFilter(Configuration config) throws IOException {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();
            System.out.printf("scan Cell with filter from table = %s \n", TABLE_NAME);

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(2);

            Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes("User00000005")));

            Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes(CF_DEFAULT)));

            Filter filter3 = new DependentColumnFilter(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_NAME), false,
                    CompareFilter.CompareOp.EQUAL, new RegexStringComparator("8$"));

            SingleColumnValueFilter filter4 = new SingleColumnValueFilter(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_NAME),
                    CompareFilter.CompareOp.EQUAL, new RegexStringComparator("9$"));
//            filter4.setFilterIfMissing(false);
            filter4.setLatestVersionOnly(false);

            FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterlist.addFilter(filter1);
//            filterlist.addFilter(filter2);
//            filterlist.addFilter(filter3);
            filterlist.addFilter(filter4);
            s.setFilter(filterlist);

            ResultScanner scanner = tt.getScanner(s);
            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_NAME);
            }
            scanner.close();
            System.out.println(" Done.");
        }
    }

    public static void getData(Configuration config) throws IOException {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            int row = 21;
            String rowKey = String.format("User%08d", row);
            Get get1 = new Get(Bytes.toBytes(rowKey));
            get1.setMaxVersions(20);

            System.out.printf("get Cell from table = %s \n", TABLE_NAME);
            Result result = tt.get(get1);
            printResult(result,CF_DEFAULT,COL_NAME);
        }
    }

    public static void getDataWithFilter(Configuration config) throws IOException {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            int row = 22;
            String rowkey = String.format("User%08d", row);
            Get get2 = new Get(Bytes.toBytes(rowkey));
            get2.setMaxVersions(20);

            Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes("User00000005")));

            Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.NOT_EQUAL,
                    new BinaryComparator(Bytes.toBytes("f3")));

            Filter filter3 = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL,
                    new BinaryComparator(Bytes.toBytes(COL_NAME)));

            Filter filter4 = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL,
                    new SubstringComparator("pan"));

            Filter filter5 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator("^\\d2$"));

            FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//            filterlist.addFilter(filter1);
//            filterlist.addFilter(filter2);
//            filterlist.addFilter(filter3);
            filterlist.addFilter(filter5);
            get2.setFilter(filterlist);

            System.out.printf("get Cell with filter from table = %s \n", TABLE_NAME);
            Result result = tt.get(get2);
            printResult(result,CF_DEFAULT,COL_AGE);
        }
    }


    public static void main(String... args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path("./", "hbase-site.xml"));
        config.addResource(new Path("./", "core-site.xml"));

//        createSchemaTables(config);
//        putData(config);
//        scanData(config);
        scanDataWithFilter(config);
//        getData(config);
//        getDataWithFilter(config);
        //modifySchema(config);
    }
}