package com.hadoop.factoryaddress;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FactoryAddressReduce  extends Reducer<Text, Text, Text, Text> {

    private int sum;
    private int count;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
        String tableSign = "";
        List<String> factorys = new ArrayList<String>();
        List<String> addresss = new ArrayList<String>();
        Iterator<Text> iterator = values.iterator();
        while(iterator.hasNext()){
            String str = iterator.next().toString();
            tableSign = str.substring(0, str.indexOf("-"));
            if(tableSign.equals("1")){
                factorys.add(str.substring(str.indexOf("-") + 1));
            } else {
                addresss.add(str.substring(str.indexOf("-") + 1));
            }
        }
        for (String factory : factorys) {
            for (String address : addresss) {
                context.write(new Text(factory), new Text(address));
            }
        }
    }
}