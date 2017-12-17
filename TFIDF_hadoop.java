import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;
import java.lang.*;

/*
 * Main class of the TFIDF MapReduce implementation.
 * Author: Deepak Gupta (email id: dgupta22@ncsu.edu)
 */
public class TFIDF {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
            System.err.println("Usage: TFIDF <input dir>");
            System.exit(1);
        }
		
		long startTime = System.currentTimeMillis();

		// Create configuration
		Configuration conf = new Configuration();
		
		// Input and output paths for each job
		Path inputPath = new Path(args[0]);
		Path wcInputPath = inputPath;
		Path wcOutputPath = new Path("output/WordCount");
		Path dsInputPath = wcOutputPath;
		Path dsOutputPath = new Path("output/DocSize");
		Path tfidfInputPath = dsOutputPath;
		Path tfidfOutputPath = new Path("output/TFIDF");
		
		// Get/set the number of documents (to be used in the TFIDF MapReduce job)
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
		String numDocs = String.valueOf(stat.length);
		conf.set("numDocs", numDocs);
		
		// Delete output paths if they exist
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(wcOutputPath))
			hdfs.delete(wcOutputPath, true);
		if (hdfs.exists(dsOutputPath))
			hdfs.delete(dsOutputPath, true);
		if (hdfs.exists(tfidfOutputPath))
			hdfs.delete(tfidfOutputPath, true);
					
		//creating a new job
    Job job = Job.getInstance(conf, "wordcount");

		//seting jar by class. It helps Hadoop to find out that which jar 
		//it should send to nodes to perform Map and Reduce tasks
    job.setJarByClass(TFIDF.class);

		//Set the Mapper for the job
    job.setMapperClass(WCMapper.class);
    
		//Set the reducer for the job
    job.setReducerClass(WCReducer.class);

		//Set the key class for the job output data
    job.setOutputKeyClass(Text.class);

		//Set the value class for job output data
    job.setOutputValueClass(IntWritable.class);

		//adding input and output paths for the job
    FileInputFormat.addInputPath(job, wcInputPath);
    FileOutputFormat.setOutputPath(job, wcOutputPath);

		//Submit the job to the cluster and wait for it to finish
    job.waitForCompletion(true);
			
		//creating a new job for calculating doc size and
		//setting the requrired parameters
    Job jobDocSize = Job.getInstance(conf, "docSize");
    jobDocSize.setJarByClass(TFIDF.class);
    jobDocSize.setMapperClass(DSMapper.class);
    jobDocSize.setReducerClass(DSReducer.class);
    jobDocSize.setOutputKeyClass(Text.class);
    jobDocSize.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(jobDocSize, dsInputPath);
    FileOutputFormat.setOutputPath(jobDocSize, dsOutputPath);
    jobDocSize.waitForCompletion(true);

		//creating a new job for calculating TFIDF and
		//setting the requrired parameters
    Job jobTfidf = Job.getInstance(conf, "tfidf");
    jobTfidf.setJarByClass(TFIDF.class);
    jobTfidf.setMapperClass(TFIDFMapper.class);
    jobTfidf.setReducerClass(TFIDFReducer.class);
    jobTfidf.setOutputKeyClass(Text.class);
    jobTfidf.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(jobTfidf, tfidfInputPath);
    FileOutputFormat.setOutputPath(jobTfidf, tfidfOutputPath);
    jobTfidf.waitForCompletion(true);		

			long endTime = System.currentTimeMillis();
			System.out.println("Total execution time: " + (endTime - startTime) );
    }
	
	/*
	 * Creates a (key,value) pair for every word in the document 
	 *
	 * Input:  ( byte offset , contents of one line )
	 * Output: ( (word@document) , 1 )
	 *
	 * word = an individual word in the document
	 * document = the filename of the document
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
		
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

				//breaking a line into tokens
		    StringTokenizer itr = new StringTokenizer(value.toString());
		    while (itr.hasMoreTokens()) {
					//getting input file name
					String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		      word.set(itr.nextToken()+"@"+fileName);

					//writing to the output file
		      context.write(word, one);
		    }
		  }
		
    }

    /*
	 * For each identical key (word@document), reduces the values (1) into a sum (wordCount)
	 *
	 * Input:  ( (word@document) , 1 )
	 * Output: ( (word@document) , wordCount )
	 *
	 * wordCount = number of times word appears in document
	 */
	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;

			//calculating the number of times a word occured in a document 
      for (IntWritable val : values) {
        sum += val.get();
      }

      result.set(sum);

			//writing to the output file
      context.write(key, result);
		
    }
	}


	/*
	 * Rearranges the (key,value) pairs to have only the document as the key
	 *
	 * Input:  ( (word@document) , wordCount )
	 * Output: ( document , (word=wordCount) )
	 */
	public static class DSMapper extends Mapper<Object, Text, Text, Text> {
		
		private final static Text result = new Text();
    private Text docName = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

				//breaking a line into tokens
		    StringTokenizer itr = new StringTokenizer(value.toString());

		    while (itr.hasMoreTokens()) {
					String[] splitArr = (itr.nextToken()).split("@");
					docName.set(splitArr[1]);
					result.set(splitArr[0]+"="+itr.nextToken());

					//writing to the output file
		      context.write(docName, result);
		    }
			
		  }
				
    }

    /*
	 * For each identical key (document), reduces the values (word=wordCount) into a sum (docSize) 
	 *
	 * Input:  ( document , (word=wordCount) )
	 * Output: ( (word@document) , (wordCount/docSize) )
	 *
	 * docSize = total number of words in the document
	 */
	public static class DSReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text result = new Text();
		private Text newKey = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
			  int sum = 0;
				int counter = 0;
				Iterator ite = values.iterator();
			
				//an array of String to store newly generated key values
				ArrayList<String> keyArray = new ArrayList<String>();

				//an array of String to store values corresponding to the newly generated keys
				ArrayList<String> valueArray = new ArrayList<String>();

				//parsing values list and calculating docsize
				//storing key-value pair in corresponding arrays
			  while(ite.hasNext()) {
					String[] splitArr = ((ite.next()).toString()).split("=");
					keyArray.add(splitArr[0]+"@"+key.toString());
					valueArray.add(splitArr[1]+"/");
					sum = sum + Integer.valueOf(splitArr[1]);
				}

				//reading from arrays and writing the output in the output file
				for(counter = 0; counter<keyArray.size(); counter++) {
					newKey.set(keyArray.get(counter));
					result.set(valueArray.get(counter)+String.valueOf(sum));

					//writing to the output file
					context.write(newKey, result);
				}						
			}
		
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the word as the key
	 * 
	 * Input:  ( (word@document) , (wordCount/docSize) )
	 * Output: ( word , (document=wordCount/docSize) )
	 */
	public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

		private final static Text result = new Text();
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

				//breaking a line into tokens
		    StringTokenizer itr = new StringTokenizer(value.toString());

		    while (itr.hasMoreTokens()) {
					String[] splitArr = (itr.nextToken()).split("@");
					word.set(splitArr[0]);
					result.set(splitArr[1]+"="+itr.nextToken());

					//writing to the output file
		      context.write(word, result);
		    }
		  }
		
    }

    /*
	 * For each identical key (word), reduces the values (document=wordCount/docSize) into a 
	 * the final TFIDF value (TFIDF). Along the way, calculates the total number of documents and 
	 * the number of documents that contain the word.
	 * 
	 * Input:  ( word , (document=wordCount/docSize) )
	 * Output: ( (document@word) , TFIDF )
	 *
	 * numDocs = total number of documents
	 * numDocsWithWord = number of documents containing word
	 * TFIDF = (wordCount/docSize) * ln(numDocs/numDocsWithWord)
	 *
	 * Note: The output (key,value) pairs are sorted using TreeMap ONLY for grading purposes. For
	 *       extremely large datasets, having a for loop iterate through all the (key,value) pairs 
	 *       is highly inefficient!
	 */
	public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
		
		private static int numDocs;
		private Map<Text, Text> tfidfMap = new HashMap<>();
		
		
		// gets the numDocs value and stores it
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			numDocs = Integer.parseInt(conf.get("numDocs"));
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	 
			//Put the output (key,value) pair into the tfidfMap instead of doing a context.write
			//tfidfMap.put(/*document@word*/, /*TFIDF*/);
			int numDocsWithWord = 0;
			int wordCount, docSize;
			double tfidfVal;

			//an array of String to store newly generated key values
			ArrayList<String> keyArr = new ArrayList<String>();

			//an array of String to store wordCount/docSize values 
			//corresponding to the newly generated keys
			ArrayList<Double> wordDocValueArr = new ArrayList<Double>();

			for(Text val : values)	{	
				numDocsWithWord++;
				String[] splitArr = (val.toString()).split("=");
				String[] wordDocArr = splitArr[1].split("/");
				wordCount = Integer.valueOf(wordDocArr[0]);
				docSize = Integer.valueOf(wordDocArr[1]);
				wordDocValueArr.add((double)wordCount/docSize);
				keyArr.add(splitArr[0]+"@"+key.toString());
			}
	
			for(int counter = 0; counter<keyArr.size(); counter++)	{
				tfidfVal = (wordDocValueArr.get(counter))* Math.log((double)numDocs/numDocsWithWord);
				tfidfMap.put(new Text (keyArr.get(counter)),new Text (String.valueOf(tfidfVal)));
			}	        			
		}
		
		// sorts the output (key,value) pairs that are contained in the tfidfMap
		protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, Text> sortedMap = new TreeMap<Text, Text>(tfidfMap);
			for (Text key : sortedMap.keySet()) {
                context.write(key, sortedMap.get(key));
            }

        }
    }
}
