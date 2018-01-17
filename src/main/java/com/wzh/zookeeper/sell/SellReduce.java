package com.wzh.zookeeper.sell;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SellReduce extends Reducer<Text, LongWritable, Text, LongWritable> {


    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,InterruptedException {
        Long sumMoney = 0L;
        for(LongWritable val:values){
            sumMoney += val.get();
        }
        context.write(key, new LongWritable(sumMoney));
    }


}