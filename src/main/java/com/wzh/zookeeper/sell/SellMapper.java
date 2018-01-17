package com.wzh.zookeeper.sell;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SellMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
        while(tokenizerArticle.hasMoreTokens()){
            StringTokenizer stringTokenizer = new StringTokenizer(tokenizerArticle.nextToken(), ",");
            String id = stringTokenizer.nextToken();
            String num = stringTokenizer.nextToken();
            Long money = Long.valueOf(stringTokenizer.nextToken());
            String time = stringTokenizer.nextToken();
            context.write(new Text(time.substring(0, 7)), new LongWritable(money));
        }
    }
}
