package com.hadoop.average;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AverageReduce  extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int sum;
    private int count;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
        for(IntWritable val:values){
            sum += val.get();
            count++;
        }
        context.write(key, new IntWritable(sum / count));
    }


}