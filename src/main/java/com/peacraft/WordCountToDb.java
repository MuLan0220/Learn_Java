package com.peacraft;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WordCountToDb {

	static class Maps extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] dataArr = value.toString().split(" ");
			if (dataArr.length > 0) {
				for (String word : dataArr) {
					context.write(new Text(word), one);
				}
			}
		}

	}

	static class Reduces extends Reducer<Text, IntWritable, WordCountTb, WordCountTb> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, WordCountTb, WordCountTb>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();

			}
			context.write(new WordCountTb(key.toString(), sum), null);
		}

	}

	public static void main(String[] args)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
//		DBConfiguration.configureDB(conf, StaticConstant.jdbcDriver, StaticConstant.jdbcUrl, StaticConstant.jdbcUser,
//				StaticConstant.jdbcPassword);
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://127.0.0.1:3306/wordcount",
				"root", "qweqwe");
		Job job = Job.getInstance(conf, "word-count");
		job.setJarByClass(WordCountToDb.class);
		String inputPath = "hdfs://master:9000/words.txt";
		if (args != null && args.length > 0) {
			inputPath = args[0];
		}
		FileInputFormat.addInputPath(job, new Path(inputPath));
		job.setMapperClass(Maps.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(Reduces.class);
		job.setOutputKeyClass(WordCountTb.class);
		job.setOutputFormatClass(DBOutputFormat.class);
		DBOutputFormat.setOutput(job, "wordcount", "name", "value");
		job.waitForCompletion(true);
	}

}
