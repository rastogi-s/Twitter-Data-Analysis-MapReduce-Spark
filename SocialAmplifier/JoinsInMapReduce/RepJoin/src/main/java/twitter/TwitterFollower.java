package twitter;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TwitterFollower extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(TwitterFollower.class);

    enum Counter {TRIANGLE, TWOPATH}

    public static class TriangleCountMapper extends Mapper<Object, Text, Text, Text> {
        private final Text resultValue = new Text();
        private final static int MAX_FILTER = 5000;
        private static Map<Integer, List<Integer>> map = new HashMap<>();

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (context.getCacheFiles() != null
                    && context.getCacheFiles().length > 0) {

//                URI[] uris = Job.getInstance(context.getConfiguration()).getCacheFiles();
//
//                for (URI uri : uris) {
                BufferedReader reader = new BufferedReader(new FileReader("edges.csv"));
                String val = reader.readLine();
                while (val != null) {
                    String[] array = val.split(",");
                    int follower = Integer.parseInt(array[0]);
                    int user = Integer.parseInt(array[1]);
                    if (follower < MAX_FILTER && user < MAX_FILTER) {
                        if (!map.containsKey(follower)) {
                            List<Integer> list = new ArrayList<>();
                            list.add(user);
                            map.put(follower, list);
                        } else {
                            List<Integer> list = map.get(follower);
                            list.add(user);
                            map.put(follower, list);
                        }
                        val = reader.readLine();
                    }
                }

                reader.close();

                //}
            }
            // super.setup(context);
        }

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String s[] = value.toString().split(",");
            int follower = Integer.parseInt(s[0]);
            int user = Integer.parseInt(s[1]);

            if (follower < MAX_FILTER && user < MAX_FILTER) {

                List<Integer> list;
                if (map.containsKey(user)) {
                    list = map.get(user);
                    for (int delta : list) {
                        if (delta < MAX_FILTER) {
                            context.getCounter(Counter.TWOPATH).increment(1);
                            if (map.containsKey(delta) && map.get(delta).contains(follower)) {
                                context.getCounter(Counter.TRIANGLE).increment(1);
                            }
                        }

                    }
                }
            }

        }


        @Override
        public void cleanup(final Context contx) throws IOException, InterruptedException {
            resultValue.set(Long.toString(contx.getCounter(Counter.TRIANGLE).getValue() / 3));
            contx.write(new Text("Number Of triangles"), resultValue);
            resultValue.set(Long.toString(contx.getCounter(Counter.TWOPATH).getValue()));
            contx.write(new Text("Number Of two paths"), resultValue);
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
        job.setMapperClass(TriangleCountMapper.class);
        // setting reducer class
        //job.setReducerClass(ReducerJoin.class);
        job.setNumReduceTasks(0);
        // setting the output key class
        job.setOutputKeyClass(Text.class);
        // setting the output value class
        job.setOutputValueClass(Text.class);

        // job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.addCacheFile(new Path(args[0] + "/edges.csv").toUri());
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
