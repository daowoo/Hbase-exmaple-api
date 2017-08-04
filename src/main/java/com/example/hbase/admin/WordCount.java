package com.example.hbase.admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    private static final IntWritable ONE = new IntWritable(1);

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);

            String line = value.toString();
            StringTokenizer token = new StringTokenizer(line);
            while (token.hasMoreTokens()) {
                word.set(token.nextToken());
                context.write(word, ONE);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);

            int count = 0;
            for (IntWritable v : values) {
                count += v.get();
            }

            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.addResource(new Path("./", "core-site.xml"));

        try {

            Job job = Job.getInstance(conf, "wordcount");

            job.setJarByClass(WordCount.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setUser("hdfs");

            Path inputpath = new Path("/tmp/111");
            Path outputpath = new Path("/tmp/222");

            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outputpath)) {
                fs.delete(outputpath, true);
            }
            FileInputFormat.addInputPath(job, inputpath);
            FileOutputFormat.setOutputPath(job, outputpath);

            boolean res = job.waitForCompletion(true);
            //System.out.printf("Map Task Done, res = %b",res);
            //System.out.println();

            //System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
