package com.example.hbase.admin;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadOnly extends TableMapper<Writable,Writable> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException, InterruptedException {
        super.map(row, columns, context);

        k.set("chenhong");
        v.set("1233212");
        context.write(k,v);

        String rowkey = new String(row.get());
        for (Cell cell : columns.listCells()) {
            String col_value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            String col_family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getValueLength());
            String col_name =  Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getValueLength());
            long timestamp = cell.getTimestamp();

            if (rowkey.startsWith("User")) {
                k.set(rowkey);
                v.set(String.format("%s:%s-%032d = %s",col_family,col_name,timestamp,col_value));

                context.write(k,v);
            }
        }
    }
}
