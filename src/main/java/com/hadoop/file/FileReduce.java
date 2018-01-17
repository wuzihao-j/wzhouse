package com.hadoop.file;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FileReduce   extends Reducer<Text, FileTime, Text, Text> {

    private int sum;
    private int count;

    @Override
    public void reduce(Text key, Iterable<FileTime> values, Context context) throws IOException,InterruptedException {
        FileTime fileTime = null;
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<FileTime> iterator = values.iterator();
        while(iterator.hasNext()){
            fileTime = iterator.next();
            stringBuilder.append(";" + fileTime.toString());
        }
        context.write(key, new Text(stringBuilder.toString().substring(1)));
    }
}