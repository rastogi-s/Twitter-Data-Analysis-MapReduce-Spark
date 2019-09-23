package twitter;

import java.io.IOException;
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
    enum Counter {UPDATEPATHS}

    static List<String> list = new ArrayList<>();


    private static final Logger logger = LogManager.getLogger(TwitterFollower.class);

    public static class ConstructAdjListMapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyMap = new Text();
        private final Text valueMap = new Text();


        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String s[] = value.toString().split(",");

            keyMap.set(s[0]);
            valueMap.set(s[1]);
            context.write(keyMap, valueMap);

        }
    }


    public static class DiameterFinderMapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyFrom = new Text();
        private final Text valueFrom = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            String s[] = value.toString().split("-");

            keyFrom.set(s[0]);
            valueFrom.set(s[1]);
            context.write(keyFrom, valueFrom);

            String arr[] = s[1].split(":");
            String isActiveSource1 = arr[2];
            String isActiveSource2 = arr[4];
            for (String node : arr[0].split(",")) {
                int distance1 = Integer.parseInt(arr[1]);
                int distance2 = Integer.parseInt(arr[3]);
                if (!node.equals("")) {
                    if (isActiveSource1.equals("1"))
                        distance1 = distance1 != Integer.MAX_VALUE ? distance1 + 1 : distance1;
                    if (isActiveSource2.equals("1"))
                        distance2 = distance2 != Integer.MAX_VALUE ? distance2 + 1 : distance2;

                    context.write(new Text(node), new Text(Integer.toString(distance1) + "&" +
                            Integer.toString(distance2)));
                }

            }


        }
    }

    public static class DiameterOutPutMapper extends Mapper<Object, Text, Text, Text> {
        private static long FINAL_SRC1_DISTANCE = 0;
        private static long FINAL_SRC2_DISTANCE = 0;

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            String s[] = value.toString().split(":");
            long distScr1 = Long.parseLong(s[1]);
            long distScr2 = Long.parseLong(s[3]);
            context.write(new Text("scr1"), new Text(Long.toString(distScr1)));
            context.write(new Text("scr2"), new Text(Long.toString(distScr2)));

        }


    }


    public static class ReduceAdjList extends Reducer<Text, Text, Text, Text> {
        private final Text resultKey = new Text();
        private final Text resultValue = new Text();


        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context)
                throws IOException, InterruptedException {

            StringBuffer nodeStr = new StringBuffer();

            for (Text val : values)
                nodeStr.append(val + ",");

            String nodes = nodeStr.toString();
            nodes = nodes.substring(0, nodes.length() - 1);

            if (key.toString().equals(list.get(0)))
                nodes += ":" + 0 + ":1:" + Integer.MAX_VALUE + ":0";
            else if (key.toString().equals(list.get(1)))
                nodes += ":" + Integer.MAX_VALUE + ":0:0:1";
            else
                nodes += ":" + Integer.MAX_VALUE + ":0:" + Integer.MAX_VALUE + ":0";

            resultKey.set(key);
            resultValue.set(nodes);
            context.write(resultKey, resultValue);
        }
    }


    public static class DiameterFinderReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

            long minDistance1 = Integer.MAX_VALUE;
            long minDistance2 = Integer.MAX_VALUE;
            long newDistance1 = Integer.MAX_VALUE;
            long newDistance2 = Integer.MAX_VALUE;
            String isActive1 = "0";
            String isActive2 = "0";
            String listAdj = "";
            String M = null;
            for (Text text : values) {
                String val = text.toString();
                if (isVertex(val)) {
                    M = val;
                    String resultArr[] = M.split(":");
                    listAdj = resultArr[0];
                    newDistance1 = Long.parseLong(resultArr[1]);
                    newDistance2 = Long.parseLong(resultArr[3]);
                    isActive1 = resultArr[2];
                    isActive2 = resultArr[4];


                } else {
                    String distancsArr[] = val.split("&");
                    long dist1 = Long.parseLong(distancsArr[0]);
                    long dist2 = Long.parseLong(distancsArr[1]);


                    if (dist1 < minDistance1)
                        minDistance1 = dist1;
                    if (dist2 < minDistance2)
                        minDistance2 = dist2;
                }

            }

            if (minDistance1 < newDistance1) {

                newDistance1 = minDistance1;
                isActive1 = "1";
                context.getCounter(Counter.UPDATEPATHS).increment(1);

            }
            if (minDistance2 < newDistance2) {
                newDistance2 = minDistance2;
                isActive2 = "1";
                context.getCounter(Counter.UPDATEPATHS).increment(1);
            }


            context.write(key, new Text(listAdj + ":" + newDistance1 + ":" + isActive1 + ":" + newDistance2 + ":" + isActive2));


        }

        private static boolean isVertex(String s) {

            String sArray[] = s.split(":");
            if (sArray.length > 1)
                return true;
            return false;
        }


    }


    public static class DiameterOutPutReducer extends Reducer<Text, Text, Text, Text> {



        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context)
                throws IOException, InterruptedException {

            long maxDist = 0;
            for(Text val:values)
            {
                long dist = Long.parseLong(val.toString());
                if(maxDist < dist)
                    maxDist = dist;

            }

            context.write(key,new Text(Long.toString(maxDist)) );
        }
    }




    @Override
    public int run(final String[] args) throws Exception {
        String awsBucketpath = "s3://hw3mapreducediametercalc/";
        //awsBucketpath = "";
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Twitter Follower");
        job.setJarByClass(TwitterFollower.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "-");
        list.add("3");
        list.add("500");
        // setting mapper class
        job.setMapperClass(ConstructAdjListMapper.class);
        // setting reducer class
        job.setReducerClass(ReduceAdjList.class);
        // setting the output key class
        job.setOutputKeyClass(Text.class);
        // setting the output value class
        job.setOutputValueClass(Text.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(awsBucketpath+args[1] + "0"));

        // return job.waitForCompletion(true) ? 0 : 1;
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        boolean check = true;
        int count = 0;

        while (check) {
            Job newJob = Job.getInstance(conf, "Graph diameter finder");
            final Configuration jobConf2 = newJob.getConfiguration();
            jobConf2.set("mapreduce.output.textoutputformat.separator", "-");
            newJob.setJarByClass(TwitterFollower.class);
            // setting mapper class
            newJob.setMapperClass(DiameterFinderMapper.class);
            // setting reducer class
            newJob.setReducerClass(DiameterFinderReducer.class);
            // setting the output key class
            newJob.setOutputKeyClass(Text.class);
            // setting the output value class
            newJob.setOutputValueClass(Text.class);
            // job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(newJob, new Path(awsBucketpath+args[1] + count));
            FileOutputFormat.setOutputPath(newJob, new Path(awsBucketpath+args[1] + (count + 1)));

            if (!newJob.waitForCompletion(true)) {
                System.exit(1);
            }
            count++;

            long vals = newJob.getCounters().findCounter(Counter.UPDATEPATHS).getValue();
            if (vals == 0)
                check = false;

        }

        Job newJob = Job.getInstance(conf, "Graph diameter finder");
        final Configuration jobConf2 = newJob.getConfiguration();
        jobConf2.set("mapreduce.output.textoutputformat.separator", "-");
        newJob.setJarByClass(TwitterFollower.class);
        // setting mapper class
        newJob.setMapperClass(DiameterOutPutMapper.class);
        // setting reducer class
        newJob.setReducerClass(DiameterOutPutReducer.class);
        // setting the output key class
        newJob.setOutputKeyClass(Text.class);
        // setting the output value class
        newJob.setOutputValueClass(Text.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(newJob, new Path(awsBucketpath+ args[1] + count));
        FileOutputFormat.setOutputPath(newJob, new Path(awsBucketpath+args[1] + (count + 1)));

        return newJob.waitForCompletion(true) ? 0: 1;
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
