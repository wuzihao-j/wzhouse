package com.hadoop.recommend.test;

import com.hadoop.recommend.RecommendMapper;
import com.hadoop.recommend.RecommendReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.FileReader;
import java.io.IOException;

public class HadoopTest {
    static  Configuration conf = new Configuration();
    static  MapDriver<Object, Text, Text, IntWritable> mapDriver;
    static  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    static   MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void init(){
        //测试mapreduce
        RecommendMapper mapper = new RecommendMapper();
        RecommendReduce reducer = new RecommendReduce();
        mapDriver = MapDriver.newMapDriver(new RecommendMapper());
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        //测试配置参数
        mapDriver.setConfiguration(conf);
        conf.set("myParameter1", "20");
        conf.set("myParameter2", "23");
    }

    @Test
    public void test() throws IOException{
        mapReduceDriver.withInput(new Text("a"), new Text("1,101,5.0\n" +
                "1,102,3.0\n" +
                "1,103,2.5\n" +
                "2,101,2.0\n" +
                "2,102,2.5\n" +
                "2,103,5.0\n" +
                "2,104,2.0\n" +
                "3,101,2.0\n" +
                "3,104,4.0\n" +
                "3,105,4.5\n" +
                "3,107,5.0\n" +
                "4,101,5.0\n" +
                "4,103,3.0\n" +
                "4,104,4.5\n" +
                "4,106,4.0\n" +
                "5,101,4.0\n" +
                "5,102,3.0\n" +
                "5,103,2.0\n" +
                "5,104,4.0\n" +
                "5,105,3.5\n" +
                "5,106,4.0"))
                .withOutput(new Text("101:101"), new IntWritable(1))
                .runTest();
    }


}
