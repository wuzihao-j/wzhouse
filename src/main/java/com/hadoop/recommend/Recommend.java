package com.hadoop.recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Recommend  {

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if(otherArgs.length != 2) {
                System.err.println("Usage: wordcount <in> <out>");
                System.exit(2);
            }
            Job job = new Job(conf, "recommend");
            job.setJarByClass(com.hadoop.recommend.Recommend.class);
            job.setMapperClass(RecommendMapper.class);
//            job.setReducerClass(RecommendReduce.class);
            // 设置输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
            job.setInputFormatClass(TextInputFormat.class);
            // 提供一个RecordWriter的实现，负责数据输出
    //        job.setOutputFormatClass(KeyValueTextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true)?0:1);
        }

}