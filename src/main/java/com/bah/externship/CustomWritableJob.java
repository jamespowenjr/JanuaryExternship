package com.bah.externship;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

// ##############
// ##############
//
// NOTE: There is new code in the run() method
//
// ##############
// ##############

public class CustomWritableJob extends Configured implements Tool {

    public static class CustomMap extends Mapper<LongWritable, Text, Text, CustomWritable> {
        private Text outKey = new Text();
        private CustomWritable outValue = new CustomWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String symbol = line[1];
            String date = line[2];
            String close = line[6];

            outKey.set(symbol);
            outValue.setText(date);
            outValue.setNumber(Double.parseDouble(close));
            context.write(outKey, outValue);
        }
    }

    public static class CustomReduce extends Reducer<Text, CustomWritable, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {

            String symbol = key.toString();

            for(CustomWritable value : values){
                String date = value.getText();
                double close = value.getNumber();
                outputKey.set(symbol);
                outputValue.set(date + "_" + close);
                context.write(outputKey, outputValue);
            }

        }
    }

    @Override
    public int run(String[] strings) throws Exception {

        Job job = new Job(getConf(), "CustomWritableJob");
        job.setJarByClass(CustomWritable.class);

        // THIS PART IS NEW!!!!!
        // ==============
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomWritable.class);
        // ==============

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(CustomMap.class);
        job.setReducerClass(CustomReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path("/user/hue/NYSE-2000-2001.tsv"));
        TextOutputFormat.setOutputPath(job, new Path("/user/hue/jobs/" + System.currentTimeMillis()));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CustomWritableJob(), args);
        System.exit(res);
    }
}
