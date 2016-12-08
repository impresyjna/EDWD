package pl.put.idss.dw.hadoop.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

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
			Mapper<Object, Text, Text, Text> {
		// ... String getInputFileName(...) { ... }
		
		private final Text matrixCell = new Text();
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			char firstLetter = ((FileSplit) context.getInputSplit()).getPath().getName().toString().charAt(0);
			if(firstLetter == 'M'){
					StringTokenizer st = new StringTokenizer(value.toString());
					String i = st.nextToken(); 
					String j = st.nextToken(); 
					String matrixValue = st.nextToken(); 
					matrixCell.set("M"+" "+i+" "+matrixValue);
					context.write(new Text(j), matrixCell);
			}
			if(firstLetter == 'N') {
					StringTokenizer st = new StringTokenizer(value.toString());
					String j = st.nextToken(); 
					String k = st.nextToken(); 
					String matrixValue = st.nextToken(); 
					matrixCell.set("N"+" "+k+" "+matrixValue);
					context.write(new Text(j), matrixCell);
			}
		}
	}

	public static class MMMultTaskReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			ArrayList<Text> mArray = new ArrayList<>(); 
			ArrayList<Text> nArray = new ArrayList<>(); 
			String outputs = "";  
			
			while(values.iterator().hasNext()) {
				Text value = values.iterator().next(); 
				String[] stringValue = value.toString().split("\\s"); 
				int cellValue = Integer.parseInt(stringValue[2]);
				if(stringValue[0].equals("M")) {
					mArray.add(value);
				} else if(stringValue[0].equals("N")) {
					nArray.add(value);
				}
			}
			
			for(int i=0; i<mArray.size(); i++) {
				System.out.println(mArray.get(i));
				for(int j=0; j<nArray.size(); j++) {
					System.out.println(nArray.get(j));
					String[] mStrings = mArray.get(i).toString().split("\\s"); 
					String[] nStrings = nArray.get(j).toString().split("\\s"); 
					int value = Integer.parseInt(mStrings[2])*Integer.parseInt(nStrings[2]); 
//					System.out.println(mStrings[1]);
//					System.out.println(nStrings[1]);
				}
			}
			for(Text mValue: mArray) {
				for(Text nValue: nArray) {
//					System.out.println(mValue);
//					System.out.println(nValue);
					String[] mStrings = mValue.toString().split("\\s"); 
					String[] nStrings = nValue.toString().split("\\s"); 
					int value = Integer.parseInt(mStrings[2])*Integer.parseInt(nStrings[2]); 
//					System.out.println(mStrings[1]);
//					System.out.println(nStrings[1]);
					outputs = outputs + mStrings[1] + "," + nStrings[1] + "," + String.valueOf(value)+" "; 
				}
			}
			outputs = outputs.substring(0, outputs.length()-1); 
			context.write(key, new Text(outputs));
			outputs = "";
		}
	}

	public static class MMMultTask2Mapper extends
			Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Text newKey = new Text();
			String[] parts = value.toString().split("\t");
			String[] values = parts[1].toString().split("\\s"); 
			for(String tuple: values) {
				String[] strings = tuple.split(",");
				newKey.set(strings[0]+","+strings[1]);
				context.write(newKey, new IntWritable(Integer.parseInt(strings[2])));
			}
		}
	}

	public static class MMMultTask2Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			result.set(0);
			int sum=0; 
			for(IntWritable element:values){
				sum += element.get(); 
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: mm-mult <input_dir> <output_dir>");
			System.exit(2);
		}

		Job job = new Job(conf, "matrix-vector-mult");
		job.setJarByClass(MMMultTask.class);

		job.setMapperClass(MMMultTaskMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MMMultTaskReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
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
		System.exit(0);
	}
}
