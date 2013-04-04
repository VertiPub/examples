package st.ata.vcc.examples.hadoop.mapred.wordcount;

/**
 * Sample code to demonstrate distributed caches with java
 *
 * Without any options, this is a standard hadoop word count example 
 * that counts the number of unique words from an input file.
 *
 * You can add the "-replace <hdfs file name>" option. The file is expected
 * to contain a series of word pairs, one pair per line i.e.
 *
 * <word1> <word1 replacement>
 * <word2> <word2 replacement>
 * :
 * :
 *
 * The example will then replace the words in the input file with the
 * words in the list.
 *
 * 
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountAndReplace extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private java.util.Map patternsToReplace = new HashMap();
        
	private long numRecords = 0;
	private String inputFile;
        
	public void configure(JobConf job) {

	    inputFile = job.get("map.input.file");
	    
	    if (job.getBoolean("wordcount.replace.patterns", false)) {
		Path[] patternsFiles = new Path[0];
		try {
		    patternsFiles = DistributedCache.getLocalCacheFiles(job);
		} catch (IOException ioe) {
                    System.err.println("Caught exception while getting files from distributed cache: " + StringUtils.stringifyException(ioe));
		}
		for (Path patternsFile : patternsFiles) {
                    parseReplaceFile(patternsFile);
		}
	    }
	}
        
	private void parseReplaceFile(Path patternsFile) {
	    try {
		BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
		String pattern = null;
		while ((pattern = fis.readLine()) != null) {
		    String line[] = pattern.split(" ");
		    if (line.length != 2) {
			System.err.println("Can't parse line from cached file: " + pattern);
		    }
                    patternsToReplace.put(line[0], line[1]);
		}
	    } catch (IOException ioe) {
		System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
	    }
	}
        

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();
	    
	    Iterator it = patternsToReplace.entrySet().iterator();

	    while (it.hasNext()) {
		java.util.Map.Entry pairs = (java.util.Map.Entry)it.next();
		String wordToReplace = (String) pairs.getKey();
		String replacementWord = (String) pairs.getValue();

		line = line.replaceAll(wordToReplace, replacementWord);
		it.remove(); // avoids a ConcurrentModificationException
	    }
	    
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    while (tokenizer.hasMoreTokens()) {
		word.set(tokenizer.nextToken());
		output.collect(word, one);
	    }
	    
	    if ((++numRecords % 100) == 0) {
		reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
	    }
	}
    }


    
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    int sum = 0;
	    while (values.hasNext()) {
		sum += values.next().get();
	    }
	    output.collect(key, new IntWritable(sum));
	}
    }


    
    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), WordCountAndReplace.class);
	conf.setJobName("wordcountandreplace");
        
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
        
	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);
        
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	List<String> other_args = new ArrayList<String>();
	for (int i=0; i < args.length; ++i) {
	    if ("-replace".equals(args[i])) {
		DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
		conf.setBoolean("wordcount.replace.patterns", true);
	    } else {
		other_args.add(args[i]);
	    }
	}
	
	FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
	FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
        
	JobClient.runJob(conf);
	return 0;
    }
    
    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new WordCountAndReplace(), args);
	System.exit(res);
    }
    
}
