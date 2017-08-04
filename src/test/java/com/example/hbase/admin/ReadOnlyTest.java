package com.example.hbase.admin;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.junit.*;
import org.junit.runners.MethodSorters;

/** 
* ReadOnly Tester. 
* 
* @author <Authors name> 
* @since <pre>3, 2017</pre>
* @version 1.0 
*/

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ReadOnlyTest extends BaseTest {

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
}

/** 
* 
* Method: map(ImmutableBytesWritable row, Result columns, Context context) 
* 
*/ 
@Test
public void testMap() throws Exception {

    try (Connection connection = ConnectionFactory.createConnection(config);
         Table tt = connection.getTable(TableName.valueOf(TABLE_NAME))) {

        Scan s = new Scan();
        s.setMaxVersions(1);
        s.addFamily(Bytes.toBytes(CF_DEFAULT));

        String start_key = buildRowKey(10);
        String end_key = buildRowKey(60);
        s.setStartRow(Bytes.toBytes(start_key));
        s.setStopRow(Bytes.toBytes(end_key));

        Path outputpath = new Path("/tmp/phf");
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        Job job = Job.getInstance(config,"ReadOnly");
        job.setJarByClass(ReadOnly.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,outputpath);

        TableMapReduceUtil.initTableMapperJob(TABLE_NAME,
                s,
                ReadOnly.class,
                Text.class,
                Text.class,
                job,
                false);

        boolean res = job.waitForCompletion(true);
        System.out.printf("Map Task Done, res = %b",res);
        System.out.println();
    }
} 


} 
