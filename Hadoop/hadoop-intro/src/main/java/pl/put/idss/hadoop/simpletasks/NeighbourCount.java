package pl.put.idss.hadoop.simpletasks;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class NeighbourCount {
	private static final String TMP_PATH= "/tmp_neighbours";

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private final Text word = new Text();
		private final Text word2 = new Text(); 

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split("\\s"); 
			for(int i=0; i<words.length; i++) {
				word.set(words[i]);
				
				if(i>0) {
					word2.set(words[i-1]); 
					context.write(word, word2);
				}
				
				if(i+1<words.length) {
					word2.set(words[i+1]);
					context.write(word, word2);
				}
			}
		}
		
	}
	
	private static class UCombiner extends Reducer<Text, Text, Text, Text> {
		@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<Text> words = new HashSet<>(); 
			
			for(Text value: values){
				if(!words.contains(value)) {
					words.add(value); 
					context.write(key, value); 
				}
			}
		}
	}
	
	private static class UReducer extends Reducer<Text, Text, Text, NullWritable> {
		private final NullWritable out = NullWritable.get(); 
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Set<Text> words = new HashSet<>(); 
			
			for(Text value: values){
				if(!words.contains(value)) {
					words.add(value); 
					context.write(key, out); 
				}
			}
		}
	}

	private static class ToIntMap extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, ONE);
        }
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: neighbourcount <in> <out>");
			System.exit(2);
		}

		//Prepare
		Job job = new Job(conf, "prepareneighbourcount");
		job.setJarByClass(NeighbourCount.class);
		
		job.setMapperClass(NeighbourCount.TokenizerMapper.class);
		job.setCombinerClass(NeighbourCount.UCombiner.class);
		job.setReducerClass(NeighbourCount.UReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(TMP_PATH));
		
		job.waitForCompletion(true); 
		
		
		Job job2 = new Job(conf,"neighbourcount"); 
		job2.setJarByClass(NeighbourCount.class);
		
		job2.setMapperClass(NeighbourCount.ToIntMap.class);
        job2.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(IntSumReducer.class);

        job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path(TMP_PATH));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		
		boolean result = job2.waitForCompletion(true);
        FileSystem.get(conf).delete(new Path(TMP_PATH), true);
			System.exit(result ? 0 : 1);
	}

}
