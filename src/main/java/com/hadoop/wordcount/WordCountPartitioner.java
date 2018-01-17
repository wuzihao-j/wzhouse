package com.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Random;

public class WordCountPartitioner extends Partitioner<Text, IntWritable>{
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        String substring = text.toString().substring(0, 1);
        if(substring.charAt(0) > 'N'){
            return 0%numPartitions;
        } else {
            return 1%numPartitions;
        }
    }
}
