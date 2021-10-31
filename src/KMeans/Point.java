package my.KMeans;

import org.apache.hadoop.io.*;


public class Point {
	private double x;
    private double y;
    private int i;
    
    public Point(double x, double y, int i) {
    	this.x = x;
    	this.y = y;
    	this.i = i;
    }
    
    public Point(Text t, IntWritable i) {
    	String s = t.toString();
    	String[] sArray = s.split(",");
    	this.x = Double.parseDouble(sArray[0]);
    	this.y = Double.parseDouble(sArray[1]);
    	this.i = i.get();
    }
    public double getx() {
    	return x;
    }
    public double gety() {
    	return y;
    }
    	
	 public int geti() {
	    return i;
}
}
