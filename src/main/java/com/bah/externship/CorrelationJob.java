package com.bah.externship;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class CorrelationJob extends Configured implements Tool {

    public static class CorrelationMap extends Mapper<LongWritable, Text, Text, Text> {
        private Text outValue = new Text();
        private Text outKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String symbol = line[1];
            String date = line[2];
            String close = line[6];

            outKey.set(symbol);
            outValue.set(date + "_" + close);
            context.write(outKey, outValue);
        }
    }

    public static class CorrelationReduce extends Reducer<Text, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String symbol = key.toString();

            Map<String,Double> datePriceMap = new TreeMap<String,Double>(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return -1 * o1.compareTo(o2);
                }
            });

            for(Text value : values){
                String[] parts = value.toString().split("_");
                datePriceMap.put(parts[0], Double.parseDouble(parts[1]));
            }

            Iterator<Entry<String, Double>> iterator = datePriceMap.entrySet().iterator();

            Entry<String,Double> current = iterator.next();
            String output = symbol;
            while(iterator.hasNext()){
                Entry<String,Double> previous = iterator.next();

                double logDiff = Math.log(current.getValue() / previous.getValue());
                output += "," + logDiff;
                current = previous;
            }

            outputKey.set(output);

            context.write(outputKey, outputValue);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {

        Job job = new Job(getConf(), "WordCount");
        job.setJarByClass(CorrelationJob.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(CorrelationMap.class);
        job.setReducerClass(CorrelationReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path("/user/hue/NYSE-2000-2001.tsv"));
        TextOutputFormat.setOutputPath(job, new Path("/user/hue/jobs/" + System.currentTimeMillis()));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CorrelationJob(), args);
        System.exit(res);
    }
}
