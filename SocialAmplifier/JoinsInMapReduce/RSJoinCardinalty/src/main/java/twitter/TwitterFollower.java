package twitter;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TwitterFollower extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(TwitterFollower.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private final Text keyFrom = new Text();
		private final Text valueFrom = new Text();
		private final Text keyTo = new Text();
		private final Text valueTo = new Text();
		private final static int MAX_FILTER = 10;

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {
			String s[] = value.toString().split(",");
			int follower = Integer.parseInt(s[0]);
			int user = Integer.parseInt(s[1]);

			if (follower < MAX_FILTER && user < MAX_FILTER) {

				keyFrom.set(Integer.toString(follower));
				keyTo.set(Integer.toString(user));
				valueFrom.set(user + ",from");
				valueTo.set(follower + ",to");
				context.write(keyFrom, valueFrom);
				context.write(keyTo, valueTo);

			}

		}
	}

	public static class ReducerJoin extends Reducer<Text, Text, Text, Text> {
		private final Text resultValue = new Text();
		private static BigInteger cardinality = BigInteger.valueOf(0);

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context)
				throws IOException, InterruptedException {
			List<String> fromList = new ArrayList<>();
			List<String> toList = new ArrayList<>();
			for (Text val : values) {
				String[] array = val.toString().split(",");
				String type = array[1];
				if (type.equals("from"))
					fromList.add(array[0]);
				else
					toList.add(array[0]);

			}

			cardinality = cardinality.add(BigInteger.valueOf(fromList.size()).multiply(BigInteger.valueOf(toList.size())));
			

		}

		@Override
		public void cleanup(final Context contx) throws IOException, InterruptedException {

			resultValue.set(cardinality.toString());
		
			contx.write(new Text("cardinality"), resultValue);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Twitter Follower");
		job.setJarByClass(TwitterFollower.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "");

		/*
		 * Delete output directory, only to ease local development; will not work on
		 * AWS.
		 *
		 */

//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}

		// setting mapper class
		job.setMapperClass(TokenizerMapper.class);
		// setting reducer class
		job.setReducerClass(ReducerJoin.class);
		// setting the output key class
		job.setOutputKeyClass(Text.class);
		// setting the output value class
		job.setOutputValueClass(Text.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
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