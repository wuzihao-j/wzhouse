package com.hadoop.log;

import sun.util.calendar.LocalGregorianCalendar;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class KPIUtil {
    public static final SimpleDateFormat FROM_FORMAT = new SimpleDateFormat(
            "dd/MMMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    public static final SimpleDateFormat TO_FORMAT = new SimpleDateFormat(
            "yyyyMMddHH");

    public static String parseIP(String line){
        return line.substring(0, line.indexOf("-") - 1).trim();
    }

    public static String parseDate(String line) throws ParseException {
        String substring = line.substring(line.indexOf("[") + 1, line.indexOf("+0800]") - 1).trim();
        Date sourceDate = FROM_FORMAT.parse(substring);
        String descDate = TO_FORMAT.format(sourceDate);
        return descDate;
    }

    public static String parseFlow(String line){
        return line.substring(line.lastIndexOf("\"") + 1).trim();
    }

    public static String parseResource(String line){
        return line.substring(line.indexOf("\"") + 1, line.lastIndexOf("\"")).trim();
    }

    public static KPI parse(String line) throws ParseException {
        KPI kpi = new KPI();
        kpi.setDate(parseDate(line));
        kpi.setIp(parseIP(line));
        kpi.setResource(parseResource(line));
        kpi.setFlow(parseFlow(line));
        return kpi;
    }

    public static void main(String[] args) throws ParseException {
        KPI kpi = parse("175.1.0.105 - - [30/May/2013:23:59:59 +0800] \"GET /static/image/smiley/default/handshake.gif HTTP/1.1\" 304 -");
        System.out.println(kpi.toString());
    }

}
