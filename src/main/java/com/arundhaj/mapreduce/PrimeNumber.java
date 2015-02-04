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

public class PrimeNumber {
	
	public static class TokenizeMaper 
			extends Mapper<Object, Text, Text, IntWritable> {
		private Text number = new Text();
		
		private final static IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				number.set(itr.nextToken());
				context.write(number, one);
			}
		}

	}
	
	public static class PrimeReducer
			extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable output = new IntWritable();
		private String strNumber = "";
		private int number = 0;
		private boolean isPrime = true;
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			isPrime = true;
			strNumber = key.toString();
			number = Integer.parseInt(strNumber);
			
			// Find if the number is prime or not
			for (int i = 2; i <= number/2; i++) {
				if(number % i == 0) {
					isPrime = false;
					break;
				}
			}
			
			output.set(isPrime ? 1 : 0);
			context.write(key, output);
		}
		
	}

	public static void main(String[] args) 
			throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Prime Number");
		job.setJarByClass(PrimeNumber.class);
		
		job.setMapperClass(PrimeNumber.TokenizeMaper.class);
		job.setCombinerClass(PrimeNumber.PrimeReducer.class);
		job.setReducerClass(PrimeNumber.PrimeReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
