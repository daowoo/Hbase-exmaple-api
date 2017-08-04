package com.example.hbase.admin;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.*;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DeleteTest extends BaseTest {

    @Test
    public void test001DeleteTable() throws Exception {

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                Assert.fail(String.format("Table %s does not exist", TABLE_NAME));
            }

            System.out.printf("Disable table. %s", tableName.getNameAsString());
            admin.disableTable(tableName);
            System.out.println(" Done.");

            System.out.printf("Delete table %s", tableName.getNameAsString());
            admin.deleteTable(tableName);
            System.out.println(" Done.");
        }
    }
}
