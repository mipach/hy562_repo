package org.myorg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import java.io.PrintWriter;
import java.io.IOException;
import java.util.regex.Pattern;

import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.lang.StringBuilder;

public class InvIndex extends Configured implements Tool {
	
	public enum Count {
		F1,F2,F3,F4,F5,F6
	};

	private static final Logger LOG = Logger.getLogger(InvIndex.class);
	private static int INDEX = 0;	
	public static void main(String [] args) throws Exception {
		int res = ToolRunner.run(new InvIndex(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Job1");

		job.addCacheFile(new Path(args[2]).toUri());		

		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.setMapperClass(Map_1.class);
		job.setReducerClass(Reduce_1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//long count1 = job.getCounters().findCounter(Count.F1).getValue();
		//System.out.println("File 100.txt" + ":" + count1);

		job.waitForCompletion(true);
		
		try (PrintWriter pw = new PrintWriter("Counters.txt")) {
		long count1 = job.getCounters().findCounter(Count.F1).getValue();
		long count2 = job.getCounters().findCounter(Count.F2).getValue();
		long count3 = job.getCounters().findCounter(Count.F3).getValue();
		long count4 = job.getCounters().findCounter(Count.F4).getValue();
		long count5 = job.getCounters().findCounter(Count.F5).getValue();
		long count6 = job.getCounters().findCounter(Count.F6).getValue();

                pw.println( count1);
		pw.println( count2);
		pw.println( count3);
		pw.println( count4);
		pw.println( count5);
		pw.println( count6);
		}
	
		return 0;
	
	}
	
	public static class Map_1 extends Mapper<LongWritable, Text, Text, Text> {
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		private Set<String> patternsToSkip = new HashSet<String>();

		protected void setup(Mapper.Context context) throws IOException, InterruptedException {
			URI[] localPaths = context.getCacheFiles();
			parseSkipFile(localPaths[0]);
		}		
		private void parseSkipFile(URI patternsURI) {
			try {
			BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
			String pattern;
			
			while((pattern = fis.readLine()) != null) {
				//split the frequency from the actual word
				String[] parts = pattern.split("\\s+");	
				patternsToSkip.add(parts[1]);
			}
			} catch (IOException ioe) {
				System.err.println("Exception on pattern");
			}
		}

	
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString().toLowerCase();
			Text currentWord = new Text();
			for(String word: WORD_BOUNDARY.split(line)) {
				if(word.isEmpty() || patternsToSkip.contains(word) || (!(word.matches("[a-zA-Z]+")))) {
					continue;
				}
				currentWord = new Text(word);
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				context.write(currentWord,new Text(fileName));
			}	
		}
	}
	public static class Reduce_1 extends Reducer <Text, Text, Text, Text> {
		public void reduce(Text word, Iterable<Text> files,Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for(Text file : files) {
				if(!(sb.toString().contains(file.toString()))) {
					switch (file.toString()) {
					case "100.txt": context.getCounter(Count.F1).increment(1); break;
					case "pg1120.txt": context.getCounter(Count.F2).increment(1); break;
					case "pg1513.txt": context.getCounter(Count.F3).increment(1); break;
					case "pg2253.txt": context.getCounter(Count.F4).increment(1); break;
					case "pg31100.txt": context.getCounter(Count.F5).increment(1); break;
					case "pg3200.txt": context.getCounter(Count.F6).increment(1); break;
					}
					sb.append(file.toString() + " ");
				}
			}
			context.write(new Text(++INDEX+" "+ word.toString()),new Text(sb.toString()));					
		}
	}
	

}
