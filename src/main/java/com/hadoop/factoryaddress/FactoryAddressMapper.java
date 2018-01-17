package com.hadoop.factoryaddress;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class FactoryAddressMapper extends Mapper<Object, Text, Text, Text> {

    private int columnNum = 0;
    private int keyIndex = 0;

    @Override
    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
        String text = value.toString();
        StringTokenizer tokenizerArticle = new StringTokenizer(text, "\n");
        while(tokenizerArticle.hasMoreTokens()) {
            String line = tokenizerArticle.nextToken();
            StringTokenizer tokenizerLine = new StringTokenizer(line, ",");
            if(columnNum == 0){
                while (tokenizerLine.hasMoreTokens()) {
                    String columnName = tokenizerLine.nextToken();
                    if (columnName.equals("addressID")) {
                        columnNum++;
                        break;
                    }
                    keyIndex++;
                }
            } else {
                String str1 = tokenizerLine.nextToken();
                String str2 = tokenizerLine.nextToken();
                if (keyIndex == 0) {
                    context.write(new Text(str1), new Text(0  + "-" + str2));
                } else {
                    context.write(new Text(str2), new Text(1 + "-" + str1));
                }
            }
        }



    }
}