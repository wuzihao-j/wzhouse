package com.hadoop.dedup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DedupReduce  extends Reducer<Text, Text, Text, Text> {

    private static int num;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
//        Iterator<Text> iterator = values.iterator();
//        while(iterator.hasNext()){
//            Text textKey = iterator.next();
//            num++;
//        }

        context.write(key, new Text(""));
    }
}
