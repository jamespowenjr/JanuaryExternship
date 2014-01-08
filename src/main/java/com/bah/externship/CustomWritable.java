package com.bah.externship;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritable implements Writable {

    private String text;
    private double number;

    public CustomWritable(){

    }

    public CustomWritable(String text, double number) {
        this.text = text;
        this.number = number;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public double getNumber() {
        return number;
    }

    public void setNumber(double number) {
        this.number = number;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(text);
        dataOutput.writeDouble(number);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        text = dataInput.readUTF();
        number = dataInput.readDouble();
    }
}
