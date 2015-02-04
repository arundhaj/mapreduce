package com.arundhaj.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EvenOdd {

	public static class TokenizeMapper
		extends Mapper<Object, Text, Text, IntWritable> {

		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class EvenOddReducer
		extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable output = new IntWritable();
		private String strNumber = "";
		private int number = 0;
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			strNumber = key.toString();
			number = Integer.parseInt(strNumber);
			output.set((number % 2 == 0) ? 1 : 0);
			
			context.write(key, output);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Even Odd");
		
		job.setJarByClass(EvenOdd.class);
		job.setMapperClass(EvenOdd.TokenizeMapper.class);
		job.setCombinerClass(EvenOdd.EvenOddReducer.class);
		job.setReducerClass(EvenOdd.EvenOddReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
