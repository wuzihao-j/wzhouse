package com.hadoop.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SortMapper  extends Mapper<Object, Text, IntWritable, IntWritable> {
    private IntWritable data=new IntWritable();
    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()) {
            String word = itr.nextToken().toString();
            data.set(Integer.parseInt(word.toString()));
            context.write(data, new IntWritable(1));
        }
    }
}
