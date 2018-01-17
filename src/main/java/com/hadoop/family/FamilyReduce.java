package com.hadoop.family;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FamilyReduce extends Reducer<Text, Text, Text, Text> {

    private int sum;
    private int count;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
        int grandChildNum = 0;
        int grandParentNum = 0;
        String[] grandChilds = new String[10];
        String[] grandParents = new String[10];
        Iterator<Text> iterator = values.iterator();
        while(iterator.hasNext()){
            String str = iterator.next().toString();
            int len = str.length();
            String sign = str.substring(0, 1);
            if(sign.equals("1")){
                grandParents[grandParentNum++] = str.substring(str.lastIndexOf("-") + 1);
            }
            if(sign.equals("2")){
                grandChilds[grandChildNum++] = str.substring(str.indexOf("-") + 1, str.lastIndexOf("-"));
            }
        }
        if(grandChildNum > 0 && grandParentNum > 0){
            for (int i = 0; i < grandParentNum; i++) {
                for (int j = 0; j < grandChildNum; j++) {
                    context.write(new Text(grandChilds[j]), new Text(grandParents[i]));
                }
            }
        }

    }
}