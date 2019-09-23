package twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TwitterFollower extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(TwitterFollower.class);

    enum Counter {UPDATEDCENTROID, SSECOUNTER}

    static int K ;

    public static class TokenizerMapperForKeyValue extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final IntWritable keyNum = new IntWritable();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String s[] = value.toString().split(",");
            int num = Integer.parseInt(s[1]);
            keyNum.set(num);
            context.write(keyNum, one);

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
            context.write(key, result);
        }
    }


    public static class CentroidMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        private static List<Integer> centroidList = new ArrayList<>();

        @Override
        protected void setup(Mapper<Object, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            URI[] files = context.getCacheFiles();
            try {
                for (URI uri : files) {

                    FileSystem fs = FileSystem.get(uri, context.getConfiguration());
                    Path filePath = new Path(uri.getPath().toString());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));

                    while (true) {
                        String val = reader.readLine();

                        if (val != null)
                            centroidList.add(Integer.parseInt(val.split("\t")[0]));
                        else
                            break;

                    }
                    reader.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            if (!centroidList.isEmpty()) {

                String arr[] = value.toString().split("\t");
                int closestCenter = centroidList.get(0);
                int minDist = Math.abs(Integer.parseInt(arr[1]) - closestCenter);

                for (int centroid : centroidList) {
                    int dist = Math.abs(Integer.parseInt(arr[1]) - centroid);
                    if (minDist > dist) {
                        closestCenter = centroid;
                        minDist = dist;
                    }
                }

                context.write(new IntWritable(closestCenter), new IntWritable(Integer.parseInt(arr[1])));

            }
        }

    }


    public static class CentroidReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private MultipleOutputs mos;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos =  new MultipleOutputs(context);

        }

        @Override
        public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            long sumOfSquaredDistance = 0;
            int center = key.get();
            List<Integer> centers = new ArrayList<>();
            for (IntWritable it : values) {

                int num = it.get();
                centers.add(num);
                sum += num;
                sumOfSquaredDistance += Math.pow((it.get() - center), 2);
                count++;
            }


            int avg = Math.abs(sum / count);
            long SSE = sumOfSquaredDistance;
            if (avg != key.get()) {
                context.getCounter(Counter.UPDATEDCENTROID).increment(1);
            }

            context.getCounter(Counter.SSECOUNTER).increment(SSE);
            mos.write("centroid",new IntWritable(avg), new Text(),"centroid/part");
            for (Integer it : centers) {

                mos.write("cluster",new IntWritable(avg), new IntWritable(it),"cluster/part");
            }



        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static class MapperGraph extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final IntWritable keyNum = new IntWritable();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String s[] = value.toString().split("\t");
            int num = Integer.parseInt(s[1]);
            keyNum.set(num);
            context.write(keyNum, one);

        }
    }

    public static class ReducerGraph extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    @Override
    public int run(final String[] args) throws Exception {

        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Twitter Follower");
        job.setJarByClass(TwitterFollower.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

        // setting mapper class
        job.setMapperClass(TokenizerMapperForKeyValue.class);
        // setting combiner class
        job.setCombinerClass(IntSumReducer.class);
        // setting reducer class
        job.setReducerClass(IntSumReducer.class);
        // setting the output key class
        job.setOutputKeyClass(IntWritable.class);
        // setting the output value class
        job.setOutputValueClass(IntWritable.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]+"/edges.csv"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }


        boolean check = true;
        int count = 0;
        K = Integer.parseInt(args[2]);

        Job initialJob = Job.getInstance(conf, "Centroid Initializer");
        final Configuration jobConfInitial = initialJob.getConfiguration();
        jobConfInitial.set("mapreduce.output.textoutputformat.separator", "\t");
        initialJob.setJarByClass(TwitterFollower.class);
        // setting mapper class
        initialJob.setMapperClass(CentroidMapper.class);
        // setting reducer class
        initialJob.setReducerClass(CentroidReducer.class);
        // setting the output key class
        initialJob.setOutputKeyClass(IntWritable.class);
        // setting the output value class
        initialJob.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get((new URI(args[1])),conf); // conf is the Configuration object

        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(args[0]+"/initial-centroids"), true);
        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();

            initialJob.addCacheFile(fileStatus.getPath().toUri());
        }


        FileInputFormat.addInputPath(initialJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(initialJob, new Path(args[1] + (count + 1)));
        MultipleOutputs.addNamedOutput(initialJob, "centroid", TextOutputFormat.class,
                IntWritable.class, Text.class);

        MultipleOutputs.addNamedOutput(initialJob, "cluster", TextOutputFormat.class,
                IntWritable.class, IntWritable.class);

        count++;
        if (!initialJob.waitForCompletion(true)) {
            System.exit(1);
        }

        System.out.println(initialJob.getCounters().findCounter(Counter.SSECOUNTER).getValue());
        while (check) {
            Job newJob = Job.getInstance(conf, "K Means");
            final Configuration jobConf2 = newJob.getConfiguration();
            jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");
            newJob.setJarByClass(TwitterFollower.class);
            // setting mapper class
            newJob.setMapperClass(CentroidMapper.class);
            // setting reducer class
            newJob.setReducerClass(CentroidReducer.class);
            // setting the output key class
            newJob.setOutputKeyClass(IntWritable.class);
            // setting the output value class
            newJob.setOutputValueClass(IntWritable.class);

            FileSystem fs2 = FileSystem.get(new URI(args[1] + count+"/centroid"),conf); // conf is the Configuration object

            //the second boolean parameter here sets the recursion to true
            RemoteIterator<LocatedFileStatus> fileStatusListIterator2 = fs2.listFiles(new Path(args[1] + count+"/centroid"), true);
            while (fileStatusListIterator2.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusListIterator2.next();
                newJob.addCacheFile(new URI(fileStatus.getPath().toString()));
            }

            FileInputFormat.addInputPath(newJob, new Path(args[1]));
            FileOutputFormat.setOutputPath(newJob, new Path(args[1] + (count + 1)));
            MultipleOutputs.addNamedOutput(newJob, "centroid", TextOutputFormat.class,
                    IntWritable.class, Text.class);

            MultipleOutputs.addNamedOutput(newJob, "cluster", TextOutputFormat.class,
                    IntWritable.class, IntWritable.class);

            if (!newJob.waitForCompletion(true)) {
                System.exit(1);
            }
            count++;

            long vals = newJob.getCounters().findCounter(Counter.UPDATEDCENTROID).getValue();
            System.out.println(newJob.getCounters().findCounter(Counter.SSECOUNTER).getValue());
            if (vals == 0)
                check = false;

            if (count > 10)
                break;


        }

        Job lastJob = Job.getInstance(conf, "Graph Plot");
        final Configuration jobConfLast = lastJob.getConfiguration();
        jobConfLast.set("mapreduce.output.textoutputformat.separator", ",");
        lastJob.setJarByClass(TwitterFollower.class);
        // setting mapper class
        lastJob.setMapperClass(MapperGraph.class);
        // setting reducer
        lastJob.setCombinerClass(ReducerGraph.class);
        // setting reducer class
        lastJob.setReducerClass(ReducerGraph.class);
        // setting the output key class
        lastJob.setOutputKeyClass(IntWritable.class);
        // setting the output value class
        lastJob.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(lastJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(lastJob, new Path(args[1]+count+1));


        return lastJob.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Three arguments required:\n<input-dir> <output-dir> <K>");
        }

        try {
            ToolRunner.run(new TwitterFollower(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}

