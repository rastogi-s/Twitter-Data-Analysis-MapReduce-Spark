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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TwitterFollower extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(TwitterFollower.class);
    private final static int MAX_FILTER = 40000;


    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyFrom = new Text();
        private final Text valueFrom = new Text();
        private final Text keyTo = new Text();
        private final Text valueTo = new Text();


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

    public static class TriangleMapperTwoPath extends Mapper<Object, Text, Text, Text> {
        private final Text keyFrom = new Text();
        private final Text valueFrom = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String s[] = value.toString().split(",");
            int follower = Integer.parseInt(s[0].trim());
            int user = Integer.parseInt(s[1].trim());

            keyFrom.set(follower + "," + user);
            valueFrom.set("from");
            context.write(keyFrom, valueFrom);

        }
    }

    public static class TriangleMapperEdges extends Mapper<Object, Text, Text, Text> {
        private final Text keyFrom = new Text();
        private final Text valueFrom = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String s[] = value.toString().split(",");
            int follower = Integer.parseInt(s[0].trim());
            int user = Integer.parseInt(s[1].trim());

            keyFrom.set(user + "," + follower);
            valueFrom.set("to");
            context.write(keyFrom, valueFrom);

        }
    }

    public static class ReducerJoin extends Reducer<Text, Text, Text, Text> {
        private final Text resultKey = new Text();
        private final Text resultValue = new Text();

        enum CounterCradinality {CARDINALITY}
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

            context.getCounter(CounterCradinality.CARDINALITY).increment(fromList.size()*toList.size());

            for (String strFrom : fromList) {
                for (String strTo : toList) {
                    resultKey.set(strTo + "," + strFrom);
                    context.write(resultKey, resultValue);
                }
            }

        }
    }

    public static class ReducerJoinTraingle extends Reducer<Text, Text, Text, Text> {
        private final Text resultValue = new Text();

        enum Counter {TRAINGLECOUNT}

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) {
            BigInteger countFrom = BigInteger.valueOf(0);
            BigInteger countTo = BigInteger.valueOf(0);
            for (Text val : values) {
                String type = val.toString().split(",")[0];
                if (type.equals("from"))
                    countFrom = countFrom.add(BigInteger.valueOf(1));
                else
                    countTo = countTo.add(BigInteger.valueOf(1));

            }


            context.getCounter(Counter.TRAINGLECOUNT).increment(countFrom.multiply(countTo).longValue());

        }

        @Override
        public void cleanup(final Context contx) throws IOException, InterruptedException {
            resultValue.set(Long.toString(contx.getCounter(Counter.TRAINGLECOUNT).getValue() / 3));
            logger.info("\n===========================\n The number of Traingles in this Job " +
                    "with max filter " + MAX_FILTER + ": " + resultValue.toString() + "\n=================");
            System.out.println("\n===========================\n The number of Traingles in this Job " +
                    "with max filter " + MAX_FILTER + ": " + resultValue.toString() + "\n=================");
            contx.write(new Text("The Number of Triangles is : "), resultValue);
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

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Triangle Finding");
        job2.setJarByClass(TwitterFollower.class);

        // Input to one of the mapper from node.csv
        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class,
                TriangleMapperTwoPath.class);
        // Input to one of the mapper from node.csv
        MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, TriangleMapperEdges.class);

        // setting reducer class
        job2.setReducerClass(ReducerJoinTraingle.class);
        // setting the output key class
        job2.setOutputKeyClass(Text.class);
        // setting the output value class
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path("output1"));

        return job2.waitForCompletion(true) ? 0 : 1;
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
