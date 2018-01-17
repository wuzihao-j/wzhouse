package com.hadoop.log;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class KPI {

    private String ip;
    private String date;
    private String resource;
    private String flow;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public String toString() {
        return "KPI{" +
                "ip='" + ip + '\'' +
                ", date='" + date + '\'' +
                ", resource='" + resource + '\'' +
                ", flow=" + flow +
                '}';
    }
}
