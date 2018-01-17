package com.hadoop.recommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class RecommendMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        String[] line = value.toString().split("\n");
        String[] line = Pattern.compile("\n").split(value.toString());
        for (int i = 0; i < line.length; i++) {
            String movie1 = line[i].split(",")[1];
            for (int j = 0; j < line.length; j++) {
                if(line[i].split(",")[0].equals(line[j].split(",")[0])){
                    String movie2 = line[j].split(",")[1];
                    context.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
                }
            }
        }
    }
}