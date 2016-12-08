package pl.put.idss.dw.hadoop.tasks;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class MMMultTask {

	public static class MMMultTaskMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		// ... String getInputFileName(...) { ... }

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
		}
	}

	public static class MMMultTaskReducer extends
			Reducer<IntWritable, Text, Text, IntWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
		}
	}

	public static class MMMultTask2Mapper extends
			Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
		}
	}

	public static class MMMultTask2Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// ...or you can use reducer from Hadoop :)
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: mm-mult <input_dir> <output_dir>");
			System.exit(2);
		}

		Job job = new Job(conf, "matrix-vector-mult");
		job.setJarByClass(MMMultTask.class);

		job.setMapperClass(MMMultTaskMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MMMultTaskReducer.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/M.txt"));
		FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/N.txt"));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "_tmp"));

		if (job.waitForCompletion(true)) {
			Job job2 = new Job(conf, "matrix-vector-mult2");
			job2.setJarByClass(MMMultTask.class);

			job2.setMapperClass(MMMultTask2Mapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);

			job2.setReducerClass(MMMultTask2Reducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job2, new Path(otherArgs[1] + "_tmp"));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
			job2.waitForCompletion(true);
		}
		System.exit(1);
	}
}
