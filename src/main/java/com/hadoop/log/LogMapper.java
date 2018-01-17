package com.hadoop.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;

public class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

    private int columnNum = 0;
    private int keyIndex = 0;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {
            KPI kpi = KPIUtil.parse(line);
            if(kpi.getResource().startsWith("GET /static") || kpi.getResource().startsWith("GET /uc_server")){
                return;
            }
            if(kpi.getResource().startsWith("GET ")){
                kpi.setResource(kpi.getResource().substring(4));
            }
            if(kpi.getResource().endsWith(" HTTP/1.1") || kpi.getResource().endsWith(" HTTP/1.0")){
                kpi.setResource(kpi.getResource().substring(0, kpi.getResource().length() - 9));
            }
            context.write(new Text(kpi.getIp() + "\t" + kpi.getResource() + "\t" + kpi.getDate()), new Text());
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}