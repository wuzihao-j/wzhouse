package com.hadoop.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class SortReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private static IntWritable num = new IntWritable(0);

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
        num.set(num.get() + 1);
        for(IntWritable val:values){
            context.write(key, num);
        }
    }



}