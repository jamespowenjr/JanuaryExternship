package com.bah.externship;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

public class CorrelationJob2 extends Configured implements Tool {

    public static class CorrelationMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text accountHolder = new Text();

        private Map<String,List<String>> accounts = new HashMap<String,List<String>>();

        @Override
        public void setup(Context context){
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path("/user/hue/accounts.txt"))));
                String line = br.readLine();
                while (line != null){
                    String[] parts = line.split(",");
                    List<String> stocks = new ArrayList<String>();
                    for(int i = 1; i < parts.length; i++){
                        stocks.add(parts[i]);
                    }
                    accounts.put(parts[0], stocks);
                    line=br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Parse input line which will be in the form of
            // SYMBOL,lnReturn,lnReturn,lnReturn,...
            String line = value.toString();
            int split = line.indexOf(",");
            String symbol = line.substring(0, split);

            // Check each account for the current symbol and emit if a match is found
            for(Entry<String,List<String>> account : accounts.entrySet()){
                if(account.getValue().contains(symbol)){
                    accountHolder.set(account.getKey());
                    // emit entry with Key = account holder's name, Value = SYMBOL,lnReturn,lnReturn,lnReturn...
                    context.write(accountHolder, value);
                }
            }
        }
    }

    public static class CorrelationReduce extends Reducer<Text, Text, Text, Text> {

        private Text outputValue = new Text();
        private static final DecimalFormat df = new DecimalFormat("#.#####");

        private static PearsonsCorrelation pearsons = new PearsonsCorrelation();
        private static StandardDeviation stdev = new StandardDeviation();

        // We're assuming a position of 1,000,000 shares on each stock
        private static final int position = 1000000;
        // We're also assuming we want a confidence of 95% along a normal distribution curve
        private static final double alpha = new NormalDistribution().inverseCumulativeProbability(0.95);

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Add stocks to a map of symbols -> list of LN Returns
            Map<String,List<Double>> stocks = new TreeMap<String,List<Double>>();

            for(Text value : values){
                String line = value.toString();
                int split = line.indexOf(",");
                String symbol = line.substring(0, split);
                String lnReturns[] = line.substring(split + 1).split(",");

                List<Double> returns = new ArrayList<Double>();
                for(String lnReturn : lnReturns){
                    returns.add(Double.parseDouble(lnReturn));
                }

                stocks.put(symbol, returns);
            }

            String output = "Portfolio Analysis \n";

            // Turn map into double array for matrix multiplication
            double[][] series = new double[stocks.size()][499];

            int row = 0;
            for(Entry<String,List<Double>> stock : stocks.entrySet()){
                int col = 0;
                for(Double price : stock.getValue()){
                    series[row][col] = price;
                    col++;
                }
                row++;
            }


        }

        // Convenience method for outputing a matrix to a String
        private String matrixToString(double[][] matrix){
            String output = "";
            for(int r = 0; r < matrix.length; r++){
                for(int s = 0; s < matrix[r].length; s++){
                    output += matrix[r][s];
                    if(s < matrix[r].length - 1){
                        output += "\t";
                    }
                }
                output += "\n";
            }
            return output;
        }
    }

    @Override
    public int run(String[] strings) throws Exception {

        Job job = new Job(getConf(), "WordCount");
        job.setJarByClass(CorrelationJob2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(CorrelationMap.class);
        job.setReducerClass(CorrelationReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path("/user/hue/NYSE-formatted.txt"));
        TextOutputFormat.setOutputPath(job, new Path("/user/hue/jobs/" + System.currentTimeMillis()));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CorrelationJob2(), args);
        System.exit(res);
    }
}
