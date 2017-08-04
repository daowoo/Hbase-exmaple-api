package com.example.hbase.admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;

public class BaseTest {
    protected static final String TABLE_NAME = "test:daowoo";
    protected static final String CF_DEFAULT = "f1";
    protected static final String COL_NAME = "name";
    protected static final String COL_AGE = "age";
    protected static final String COL_DESC = "desc";
    protected static final int    CELL_VERSION = 30;

    protected static Configuration config = null;

    @BeforeClass
    public static void beforeclass() throws Exception {
        config = HBaseConfiguration.create();

        config.addResource(new Path("./", "hbase-site.xml"));
        config.addResource(new Path("./", "core-site.xml"));

        System.out.println("init config ok");
    }

    protected static void createOrOverwrite(Admin admin, HTableDescriptor table) throws Exception {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    protected static String buildRowKey(int rowNum) throws Exception {
        return String.format("User%08d", rowNum);
    }

    protected static void printResult(Result result, String fm, String col) throws Exception {

        System.out.println(result);

        List<Cell> cells = result.getColumnCells(Bytes.toBytes(fm), Bytes.toBytes(col));
        for (Cell cell : cells) {
            String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            System.out.printf("%s:%s: %s timestamp: %d",fm,col,val,cell.getTimestamp());
            System.out.print(" | ");
        }
        System.out.println();
    }
}
