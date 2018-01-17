package com.hadoop.log;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReduce extends Reducer<Object, Text, Text, Text> {

    @Override
    public void reduce(Object key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
        for(Text value : values){
            context.write(new Text(key.toString()), new Text());
        }
    }
}