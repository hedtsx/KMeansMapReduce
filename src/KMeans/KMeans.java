package my.KMeans;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;


import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class KMeans {


	public static class PMapper extends Mapper<LongWritable, Text, Text, Text> {	
		
		private static List<Point> centers = new ArrayList<Point>();
		
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
	              throws IOException,
                  InterruptedException{

            List<Point> temp = new ArrayList<Point>();
			Configuration conf = context.getConfiguration();

			Path center_path = new Path(conf.get("centroid.path"));

			FileSystem fs = FileSystem.get(conf);
			
			// create a file reader
			
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, center_path, conf);

			Text key = new Text();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				temp.add(new Point(key, value));
			}
			reader.close();
			centers = temp;
			
		}

		public void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] sArray = line.split(",");
			double minD = Double.POSITIVE_INFINITY;
			int pNumber = centers.size();
			for(Point p : centers) {
				double x1 = Double.parseDouble(sArray[0]);
				double y1 = Double.parseDouble(sArray[1]);
				double dist = Math.sqrt(Math.pow(p.getx()-x1, 2)+Math.pow(p.gety()-y1, 2));
				if(dist<minD) {
					minD = dist;
					pNumber = p.geti();
				}
			}
			context.write(new Text(Integer.toString(pNumber)), new Text(line));

		}
/***
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {

		}
***/
	}


	public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
		
		private static List<Point> centers = new ArrayList<Point>();
		public static enum Counter {
			CONVERGED
		}
		// new_centroids (variable to store the new centroids

		@Override
		public void setup(Context context) throws IOException {
		    List<Point> temp = new ArrayList<Point>();
			Configuration conf = context.getConfiguration();

			Path center_path = new Path(conf.get("centroid.path"));

			FileSystem fs = FileSystem.get(conf);
			
			// create a file reader
			
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, center_path, conf);

			Text key = new Text();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				temp.add(new Point(key, value));
			}
			reader.close();
			centers = temp;
			
			Path prev_center_path = new Path("centroid/cen_prev.seq");
			//SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf);
			
			final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, prev_center_path, Text.class,
					IntWritable.class);
			for (Point p: centers) {
				double x = p.getx();
				double y = p.gety();
				String centerline = Double.toString(x)+ "," + Double.toString(y);
				Integer position = p.geti();
				centerWriter.append(new Text(centerline), new IntWritable(position));
			}
			
	
			centerWriter.close();
			
			

		}

		public void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException,InterruptedException  {
			
			double xsum = 0.0;
			double ysum = 0.0;
			int num_element = 0;
			while(values.iterator().hasNext()) {
				Text t = values.iterator().next();
				Point p = new Point(t,new IntWritable(Integer.parseInt(key.toString())));
				if (!centers.contains(p)) {
					String point = t.toString();
					String[] sArray = point.split(",");
					xsum += Double.parseDouble(sArray[0]);
					ysum += Double.parseDouble(sArray[1]);
					num_element += 1;
				}
			}
			double xcen = xsum/num_element;
			double ycen = ysum/num_element;
			Point p = new Point(xcen,ycen,Integer.parseInt(key.toString()));
			centers.set(p.geti(), p);
			String centroidpoint = Double.toString(xcen) + "," + Double.toString(ycen);

			context.write(NullWritable.get(), new Text(centroidpoint));
			

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();

			FileSystem fs = FileSystem.get(conf);
			Path center_path = new Path(conf.get("centroid.path"));

			//SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf);
			final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center_path, Text.class,
					IntWritable.class);
			for (Point p: centers) {
				double x = p.getx();
				double y = p.gety();
				String centerline = Double.toString(x)+ "," + Double.toString(y);
				Integer position = p.geti();
				centerWriter.append(new Text(centerline), new IntWritable(position));
			}
			
	
			centerWriter.close();

		}


	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Path center_path = new Path("centroid/cen.seq");
		conf.set("centroid.path", center_path.toString());

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(center_path)) {
			fs.delete(center_path, true);
		}
		//SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf);
		final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center_path, Text.class,
				IntWritable.class);
		
		// Hard Coded Randomly Selecting 5 Initial Points
		centerWriter.append(new Text("49.04816562011978,34.599014498332885"), new IntWritable(0));
		centerWriter.append(new Text("7.969646145588353,16.65864323587064"), new IntWritable(1));
		centerWriter.append(new Text("14.894067089009027,20.548709858270744"), new IntWritable(2));
		centerWriter.append(new Text("9.01365480195167,11.136407482267263"), new IntWritable(3));
		centerWriter.append(new Text("-0.9977353454805304,20.972708852330552"), new IntWritable(4));
		

		centerWriter.close();
		

		Path src = new Path(args[1]);
		fs.mkdirs(src);
		
		int itr = 0;
		long start = System.currentTimeMillis();

		while (itr < 20) {
			System.out.println("iteration No." + Integer.toString(itr));
			Job job = new Job(conf,"KMeans");
			job.setJarByClass(KMeans.class);
	        job.setMapperClass(PMapper.class);
	        job.setReducerClass(Reduce.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        job.setInputFormatClass(TextInputFormat.class);
	        FileInputFormat.addInputPath(job,new Path(args[0]));
	        TextOutputFormat.setOutputPath(job,new Path(args[1] + "/iter" + Integer.toString(itr)));
	        int code=job.waitForCompletion(true)?0:1;
	        itr += 1;

		}
		
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, center_path, conf);

		Text key = new Text();
		IntWritable value = new IntWritable();
		System.out.println("Centroids: ");
		while (reader.next(key, value)) {
			System.out.println("Centroid" + Integer.toString(value.get())+ ": "+key.toString());
		}
		reader.close();
		// some time passes
		long end = System.currentTimeMillis();
		long elapsedTime = end - start;
		
		System.out.println("Total Elapsed Time: " + Long.toString(elapsedTime) + "miliseconds");

	}


}

