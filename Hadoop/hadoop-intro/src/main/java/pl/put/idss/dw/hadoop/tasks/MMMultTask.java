package pl.put.idss.dw.hadoop.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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

	public static class MMMultTaskReducer extends Reducer<Text, Text, Text, Text> {
        private Map<Integer, Integer> mMap = new HashMap<>();
        private Map<Integer, Integer> nMap = new HashMap<>();
        private Text output = new Text();
        private String outputString;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws
                IOException, InterruptedException {
            outputString = "";
            mMap.clear();
            nMap.clear();

            while(values.iterator().hasNext()) {
                String[] result = values.iterator().next().toString().split("\\s");
                if(result[0].equals("M")) {
                    this.mMap.put(Integer.parseInt(result[1]), Integer.parseInt(result[2]));
                } else {
                    this.nMap.put(Integer.parseInt(result[1]), Integer.parseInt(result[2]));
                }
            }

            for(Map.Entry<Integer, Integer> m: mMap.entrySet()) {
                for(Map.Entry<Integer, Integer> n: nMap.entrySet()) {
                    int result = m.getValue() * n.getValue();
                    outputString += m.getKey() + " " + n.getKey() + " " + result + ",";
                }
            }

            outputString = outputString.substring(0, outputString.length() - 1);
            this.output.set(outputString);
            context.write(key, this.output);
		}
	}	
	

	public static class MMMultTask2Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text keyText = new Text();
        private IntWritable valInt = new IntWritable();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            String[] tripples = parts[1].split(",");

            for(String tripple: tripples) {
                String[] values = tripple.split("\\s");
                this.keyText.set(values[0] + " " + values[1]);
                this.valInt.set(Integer.parseInt(values[2]));
                context.write(this.keyText, this.valInt);
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
