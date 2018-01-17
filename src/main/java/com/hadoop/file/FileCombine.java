package com.hadoop.file;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FileCombine extends Reducer<Text, FileTime, Text, FileTime> {

    @Override
    public void reduce(Text key, Iterable<FileTime> values, Context context) throws IOException,InterruptedException {
        int sum = 0;
        FileTime fileTime = null;
        Iterator<FileTime> iterator = values.iterator();
        while(iterator.hasNext()){
            fileTime = iterator.next();
            sum++;
        }
        fileTime.setTimes(sum);
        context.write(key, fileTime);
    }
}