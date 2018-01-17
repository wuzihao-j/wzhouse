package com.hadoop.file;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FileTime implements Writable {

    private String fileName;
    private int times;

    public FileTime() {
    }

    public FileTime(String fileName, int times) {
        this.fileName = fileName;
        this.times = times;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getTimes() {
        return times;
    }

    public void setTimes(int times) {
        this.times = times;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.fileName);
        out.writeInt(this.times);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.fileName = in.readUTF();
        this.times = in.readInt();
    }

    @Override
    public String toString() {
        return "\"" + fileName + ":" +times + "\"";
    }
}
