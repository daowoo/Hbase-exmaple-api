package com.example.hbase.admin;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FilterTest extends BaseTest {

    @Test
    public void test001RowFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(5);

            Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes("User00000100")));

            Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator("1"));

            Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator("[5-9]$"));

            FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterlist.addFilter(filter1);
            filterlist.addFilter(filter2);
            filterlist.addFilter(filter3);
            s.setFilter(filterlist);

            System.out.printf("Scan Cell with RowFilter from table = %s", TABLE_NAME);
            ResultScanner scanner = tt.getScanner(s);
            System.out.println(" Done.");

            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_NAME);
            }
        }
    }

    @Test
    public void test002FamilyFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            int row = 251;
            Get get = new Get(Bytes.toBytes(buildRowKey(row)));
            get.setMaxVersions(15);

            Filter filter1 = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator("f"));

            Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator("1$"));

            FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterlist.addFilter(filter1);
            filterlist.addFilter(filter2);
            get.setFilter(filterlist);

            System.out.printf("Get Cell with FamilyFilter from table = %s \n", TABLE_NAME);
            Result result = tt.get(get);
            printResult(result,CF_DEFAULT,COL_NAME);
        }
    }

    @Test
    public void test003QualifierFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            int row = 252;
            Get get = new Get(Bytes.toBytes(buildRowKey(row)));
            get.setMaxVersions(10);

            Filter filter1 = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes(COL_DESC)));

            Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator("^a"));

            FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            filterlist.addFilter(filter1);
            filterlist.addFilter(filter2);
            get.setFilter(filterlist);

            System.out.printf("Get Cell with QualifierFilter from table = %s \n", TABLE_NAME);
            Result result = tt.get(get);
            printResult(result,CF_DEFAULT,COL_NAME);
        }
    }

    @Test
    public void test004ValueFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            int row = 253;
            Get get = new Get(Bytes.toBytes(buildRowKey(row)));
            get.setMaxVersions(CELL_VERSION);

            Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator("daowoo"));

            Filter filter2 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator("[6-9]$"));

            FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            filterlist.addFilter(filter1);
            filterlist.addFilter(filter2);
            get.setFilter(filterlist);

            System.out.printf("Get Cell with ValueFilter from table = %s \n", TABLE_NAME);
            Result result = tt.get(get);
            printResult(result,CF_DEFAULT,COL_NAME);
        }
    }

    @Test
    public void test005DependentColumnFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(20);

            Filter filter1 = new DependentColumnFilter(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_NAME), false,
                    CompareFilter.CompareOp.EQUAL, new RegexStringComparator("8$"));

            FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterlist.addFilter(filter1);
            s.setFilter(filterlist);

            System.out.printf("Scan Cell with DependentColumnFilter from table = %s", TABLE_NAME);
            ResultScanner scanner = tt.getScanner(s);
            System.out.println(" Done.");

            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_DESC);
            }
        }
    }

    @Test
    public void test006SingleColumnValueFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(20);

            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_NAME),
                    CompareFilter.CompareOp.EQUAL, new RegexStringComparator("9$"));
            filter1.setLatestVersionOnly(false);

            FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterlist.addFilter(filter1);
            s.setFilter(filterlist);

            System.out.printf("Scan Cell with SingleColumnValueFilter from table = %s", TABLE_NAME);
            ResultScanner scanner = tt.getScanner(s);
            System.out.println(" Done.");

            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_DESC);
            }
        }
    }

    @Test
    public void test007SingleColumnValueExcludeFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(20);

            SingleColumnValueExcludeFilter filter1 = new SingleColumnValueExcludeFilter(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_NAME),
                    CompareFilter.CompareOp.EQUAL, new RegexStringComparator("9$"));
            filter1.setLatestVersionOnly(false);

            FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterlist.addFilter(filter1);
            s.setFilter(filterlist);

            System.out.printf("Scan Cell with SingleColumnValueExcludeFilter from table = %s", TABLE_NAME);
            ResultScanner scanner = tt.getScanner(s);
            System.out.println(" Done.");

            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_DESC);
            }
        }
    }

    @Test
    public void test008PrefixFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));

            String start_key = buildRowKey(10);
            String end_key = buildRowKey(100);

            s.setStartRow(Bytes.toBytes(start_key));
            s.setStopRow(Bytes.toBytes(end_key));

            PrefixFilter filter1 = new PrefixFilter(Bytes.toBytes("User"));

            FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterlist.addFilter(filter1);
            s.setFilter(filterlist);

            System.out.printf("Scan Cell with PrefixFilter from table = %s", TABLE_NAME);
            ResultScanner scanner = tt.getScanner(s);
            System.out.println(" Done.");

            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_DESC);
            }
        }
    }

    @Test
    public void test009ColumnPrefixFilter () throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(10);

            String start_key = buildRowKey(10);
            String end_key = buildRowKey(100);

            s.setStartRow(Bytes.toBytes(start_key));
            s.setStopRow(Bytes.toBytes(end_key));

            ColumnPrefixFilter filter1 = new ColumnPrefixFilter(Bytes.toBytes("na"));
            ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("ag"));

            FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            filterlist.addFilter(filter1);
            filterlist.addFilter(filter2);
            s.setFilter(filterlist);

            System.out.printf("Scan Cell with ColumnPrefixFilter from table = %s", TABLE_NAME);
            ResultScanner scanner = tt.getScanner(s);
            System.out.println(" Done.");

            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_DESC);
            }
        }
    }

    @Test
    public void test010KeyOnlyFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(5);

            String start_key = buildRowKey(30);
            String end_key = buildRowKey(120);

            s.setStartRow(Bytes.toBytes(start_key));
            s.setStopRow(Bytes.toBytes(end_key));

            KeyOnlyFilter filter1 = new KeyOnlyFilter();
            s.setFilter(filter1);

            System.out.printf("Scan Cell with KeyOnlyFilter from table = %s", TABLE_NAME);
            ResultScanner scanner = tt.getScanner(s);
            System.out.println(" Done.");

            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_DESC);
            }
        }
    }

    @Test
    public void test011FirstKeyOnlyFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(15);

            String start_key = buildRowKey(30);
            String end_key = buildRowKey(120);

            s.setStartRow(Bytes.toBytes(start_key));
            s.setStopRow(Bytes.toBytes(end_key));

            FirstKeyOnlyFilter filter1 = new FirstKeyOnlyFilter();
            s.setFilter(filter1);

            System.out.printf("Scan Cell with FirstKeyOnlyFilter from table = %s", TABLE_NAME);
            ResultScanner scanner = tt.getScanner(s);
            System.out.println(" Done.");

            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_NAME);
            }
        }
    }

    @Test
    public void test012InclusiveStopFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(15);

            String start_key = buildRowKey(30);
            String end_key = buildRowKey(120);

            s.setStartRow(Bytes.toBytes(start_key));
            s.setStopRow(Bytes.toBytes(end_key));

            InclusiveStopFilter filter1 = new InclusiveStopFilter(Bytes.toBytes(end_key));
            s.setFilter(filter1);

            System.out.printf("Scan Cell with InclusiveStopFilter from table = %s", TABLE_NAME);
            ResultScanner scanner = tt.getScanner(s);
            System.out.println(" Done.");

            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_NAME);
            }
        }
    }

    @Test
    public void test013RandomRowFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(15);

            float chance = 0.5f;
            RandomRowFilter filter1 = new RandomRowFilter(chance);
            s.setFilter(filter1);

            System.out.printf("Scan Cell with RandomRowFilter from table = %s", TABLE_NAME);
            ResultScanner scanner = tt.getScanner(s);
            System.out.println(" Done.");

            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_NAME);
            }
        }
    }

    @Test
    public void test014ColumnCountGetFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Scan s = new Scan();

            s.addFamily(Bytes.toBytes(CF_DEFAULT));
            s.setMaxVersions(15);

            String start_key = buildRowKey(30);
            String end_key = buildRowKey(120);

            s.setStartRow(Bytes.toBytes(start_key));
            s.setStopRow(Bytes.toBytes(end_key));

            ColumnCountGetFilter filter1 = new ColumnCountGetFilter(2);
            s.setFilter(filter1);

            System.out.printf("Scan Cell with ColumnCountGetFilter from table = %s", TABLE_NAME);
            ResultScanner scanner = tt.getScanner(s);
            System.out.println(" Done.");

            for (Result result : scanner) {
                printResult(result,CF_DEFAULT,COL_NAME);
            }
        }
    }

    @Ignore
    public void test015PageFilter() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            long pageSize = 10;
            PageFilter filter1 = new PageFilter (pageSize);

            int totalRows = 0;
            byte[] lastRow = null;
            byte[] postfix = {};
            while (true) {
                Scan s = new Scan();
                s.setMaxVersions(20);
                s.setFilter(filter1);

                if (lastRow != null) {
                    byte[] startRow = Bytes.add(lastRow, postfix);
                    s.setStartRow(startRow);
                }

                ResultScanner scanner = tt.getScanner(s);

                int localRowsCount = 0;
                for (Result result : scanner) {
                    System.out.printf(localRowsCount++ + ":");
                    printResult(result,CF_DEFAULT,COL_NAME);
                    System.out.println();
                    totalRows++;
                    lastRow = result.getRow();
                }
                scanner.close();
                if(localRowsCount == 0) break;
            }

            System.out.println("Total rows is : " + totalRows);
        }
    }
}
