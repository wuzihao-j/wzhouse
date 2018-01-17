package com.hadoop.file;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class FileMapper extends Mapper<Object, Text, Text, FileTime> {

    private int columnNum = 0;
    private int keyIndex = 0;

    @Override
    public void map(Object key, Text value, Mapper<Object, Text, Text, FileTime>.Context context) throws IOException,InterruptedException {
        FileSplit fileSplit=(FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        String text = value.toString();
        StringTokenizer tokenizerArticle = new StringTokenizer(text, "\n");
        while(tokenizerArticle.hasMoreTokens()) {
            String line = tokenizerArticle.nextToken();
            StringTokenizer stringTokenizer = new StringTokenizer(line);
            while(stringTokenizer.hasMoreTokens()){
                String word = stringTokenizer.nextToken();
                context.write(new Text(word), new FileTime(fileName, 1));
            }
        }
    }
}