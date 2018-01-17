package com.hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    IntWritable one = new IntWritable(1);
    AscWriteable word = new AscWriteable();
//统计单词数
//    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
//
//        StringTokenizer itr = new StringTokenizer(value.toString());
//        while(itr.hasMoreTokens()) {
//            word.set(itr.nextToken());
//            context.write(word, one);
//        }
//    }
    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {

//        StringTokenizer itr = new StringTokenizer(value.toString());
//        while(itr.hasMoreTokens()) {
//            word.set(itr.nextToken());
//            context.write(word, one);
//        }
        context.write(new Text(value.toString()), new IntWritable());
    }
}