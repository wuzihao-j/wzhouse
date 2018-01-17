package com.hadoop.family;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class FamilyMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
//        String text = value.toString();
//        StringTokenizer tokenizerArticle = new StringTokenizer(text, "\n");
//        while(tokenizerArticle.hasMoreTokens()){
//            String line = tokenizerArticle.nextToken();
//            StringTokenizer tokenizerLine = new StringTokenizer(line);
//            String child = tokenizerLine.nextToken();
//            String parent = tokenizerLine.nextToken();
//            context.write(new Text(child), new Text(1 + "-" + child + "-" + parent));
//            context.write(new Text(parent), new Text(2 + "-" + child + "-" + parent));
//        }
        String strKey = key.toString();
        String strValue = value.toString();
        context.write(new Text(strKey), new Text(1 + "-" + strKey + "-" + strValue));
        context.write(new Text(strValue), new Text(2 + "-" + strKey + "-" + strValue));
//        context.write(new Text(strKey), new Text(strValue));
    }
}
