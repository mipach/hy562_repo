package org.myorg;

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

public class WordCount extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(WordCount.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    //Configuration conf = getConf();
    Job job = Job.getInstance(getConf(), "Job1");
    //Job job = new Job(conf,"Job1");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
   // FileOutputFormat.setOutputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job,new Path("tempOut"));
    job.setNumReduceTasks(50);
    job.setMapperClass(Map_1.class);
    job.setReducerClass(Reduce_1.class);
    //job.setCombinerClass(Reduce_1.class);
    //getConf().set("mapreduce.map.output.compress", "true");
    //getConf().set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.waitForCompletion(true);

    //Job job2 = new Job(conf,"Job2");
    Job job2 = Job.getInstance(getConf(),"Job2");
    job2.setJarByClass(this.getClass());
    job2.setMapperClass(Map_2.class);
    job2.setReducerClass(Reduce_2.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path("tempOut"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    return job2.waitForCompletion(true) ? 0 : 1;

  }
 public static class Map_2 extends Mapper<LongWritable, Text, IntWritable, Text> {
  private Text word = new Text();

  public void map(LongWritable offset, Text lineText,Context context) throws IOException, InterruptedException {
	String line = lineText.toString();
	String[] parts = line.split("\\s+");
	int freq = Integer.parseInt(parts[1]);
	if(freq > 4000) {
		context.write(new IntWritable(freq), new Text(parts[0]));
	}	
  }
}
  public static class Reduce_2 extends Reducer<Text, IntWritable, Text, Text> {
   //@Override
    public void reduce(Text freq, Iterable<Text> words, Context context) throws IOException, InterruptedException{
     for(Text word: words) {
		context.write(freq,word);
	}
   }
  }
 


  public static class Map_1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
    private boolean caseSensitive = false;
    //private String elem[] = {".","!","?","@",":","\"","'","(",")",",",","};
    //private Set<String> patternsToSkip = new HashSet(Arrays.asList(elem));
   // protected void setup(Mapper.Context context) throws IOException, InterruptedException {
   // 	Configuration config = context.getConfiguration();
   // 	this.caseSensitive = config.getBoolean("wordcount.case.sensitive",false);
   // }
    
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
     // if (!caseSensitive) {
      line = line.toLowerCase();
     // }
      Text currentWord = new Text();
      for (String word : WORD_BOUNDARY.split(line)) {
        if (word.isEmpty() || (!(word.matches("[a-zA-Z]+"))) ) { //|| patternsToSkip.contains(word) ) {
            continue;
        }
            currentWord = new Text(word);
            context.write(currentWord,one);
        }
    }
  }

  public static class Reduce_1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
}
