package wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TwitterFollower extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(TwitterFollower.class);

	public static class TokenizerMapperForKeys extends Mapper<Object, Text, IntWritable, IntWritable> {
		private final static IntWritable zero = new IntWritable(0);
		private final  IntWritable keyNum = new IntWritable();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				int num = Integer.parseInt(itr.nextToken());
				keyNum.set(num);
				context.write(keyNum, zero);
			}
		}
	}

	public static class TokenizerMapperForKeyValue extends Mapper<Object, Text, IntWritable, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final  IntWritable keyNum = new IntWritable();
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
				String s[] =value.toString().split(",");
				int num = Integer.parseInt(s[1]);
				keyNum.set(num);
				context.write(keyNum,one);
			
		}
	}
	
	public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key,result);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Twitter Follower");
		job.setJarByClass(TwitterFollower.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		
		/* 
		 * Delete output directory, only to ease local development;
		 * will not work on AWS.
		 *  
		 */
		
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		
		// Input to one of the mapper from node.csv
		MultipleInputs.addInputPath(job,new Path(args[0]+"/nodes.csv"), TextInputFormat.class, TokenizerMapperForKeys.class);
		//  Input to one of the mapper from node.csv
		MultipleInputs.addInputPath(job, new Path(args[0]+"/edges.csv"), TextInputFormat.class, TokenizerMapperForKeyValue.class);
		// setting combiner class
		job.setCombinerClass(IntSumReducer.class);
		// setting reducer class
		job.setReducerClass(IntSumReducer.class);
		// setting the output key class
		job.setOutputKeyClass(IntWritable.class);
		// setting the output value class
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new TwitterFollower(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}