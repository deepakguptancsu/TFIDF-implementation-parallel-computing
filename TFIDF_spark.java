import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/*
 * Main class of the TFIDF Spark implementation.
 * Author: Deepak Gupta (email id: dgupta22@ncsu.edu)
 */
public class TFIDF {

	static boolean DEBUG = false;

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
            System.err.println("Usage: TFIDF <input dir>");
            System.exit(1);
        }
		
		long startTime = System.currentTimeMillis();

		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setAppName("TFIDF");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data
		// Output is: ( filePath , fileContents ) for each file in inputPath
		String inputPath = args[0];
		JavaPairRDD<String,String> filesRDD = sc.wholeTextFiles(inputPath);
		
		// Get/set the number of documents (to be used in the IDF job)
		long numDocs = filesRDD.count();
		
		//Print filesRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = filesRDD.collect();
			System.out.println("------Contents of filesRDD------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2.trim() + ")");
			}
			System.out.println("--------------------------------");
		}
		
		/* 
		 * Initial Job
		 * Creates initial JavaPairRDD from filesRDD
		 * Contains each word@document from the corpus and also attaches the document size for 
		 * later use
		 * 
		 * Input:  ( filePath , fileContents )
		 * Map:    ( (word@document) , docSize )
		 */
		JavaPairRDD<String,Integer> wordsRDD = filesRDD.flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,String>,String,Integer>() {
				public Iterable<Tuple2<String,Integer>> call(Tuple2<String,String> x) {
					// Collect data attributes
					String[] filePath = x._1.split("/");
					String document = filePath[filePath.length-1];
					String fileContents = x._2;
					String[] words = fileContents.split("\\s+");
					int docSize = words.length;
					
					// Output to Arraylist
					ArrayList ret = new ArrayList();
					for(String word : words) {
						ret.add(new Tuple2(word.trim() + "@" + document, docSize));
					}
					return ret;
				}
			}
		);
		
		//Print wordsRDD contents
		if (DEBUG) {
			List<Tuple2<String, Integer>> list = wordsRDD.collect();
			System.out.println("------Contents of wordsRDD------");
			for (Tuple2<String, Integer> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}		
		
		/* 
		 * TF Job (Word Count Job + Document Size Job)
		 * Gathers all data needed for TF calculation from wordsRDD
		 *
		 * Input:  ( (word@document) , docSize )
		 * Map:    ( (word@document) , (1/docSize) )
		 * Reduce: ( (word@document) , (wordCount/docSize) )
		 */
		
		JavaPairRDD<String,String> tfRDD = wordsRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Integer>,String,String>() {
					public Iterable<Tuple2<String,String>> call(Tuple2<String,Integer> x) {
						// Collect data attributes
						int value = x._2;
						String valueStr = Integer.toString(value);
						valueStr = "1/" + valueStr;

						// Output to Arraylist
						ArrayList ret = new ArrayList();
						ret.add(new Tuple2(x._1, valueStr));
		
						return ret;
					}
				}).reduceByKey(new Function2<String, String, String>() {
				    //@Override
				    public String call(String v1,String v2) {
							//collecting values corresponding to a key and performing
							//their sum and returning
				    	String[] firstValue = v1.split("/");
				    	String[] secondValue = v2.split("/");
				    	int combinedVal = Integer.valueOf(firstValue[0]) + Integer.valueOf(secondValue[0]);
				    	String finalVal = Integer.toString(combinedVal)+ "/" + firstValue[1];
				    	return (finalVal);
				    }
				});

		
		//Print tfRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = tfRDD.collect();
			System.out.println("-------Contents of tfRDD--------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}
		
		/*
		 * IDF Job
		 * Gathers all data needed for IDF calculation from tfRDD
		 *
		 * Input:  ( (word@document) , (wordCount/docSize) )
		 * Map:    ( word , (1/document) )
		 * Reduce: ( word , (numDocsWithWord/document1,document2...) )
		 * Map:    ( (word@document) , (numDocs/numDocsWithWord) )
		 */
		JavaPairRDD<String,String> idfRDD = tfRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,String>,String,String>() {
					public Iterable<Tuple2<String,String>> call(Tuple2<String,String> x) {
						// Collect data attributes
						String[] arr1 = x._1.split("@");

						// Output to Arraylist
						ArrayList ret = new ArrayList();
						ret.add(new Tuple2(arr1[0], "1/"+arr1[1]));
						return ret;
					}
				}).reduceByKey(new Function2<String, String, String>() {
				    //@Override
				    public String call(String v1,String v2) {	
							//collecting values corresponding to a key
				    	String[] firstValue = v1.split("/");
				    	String[] secondValue = v2.split("/");

							//summing to find numDocsWithWord
				    	int combinedVal = Integer.valueOf(firstValue[0]) + Integer.valueOf(secondValue[0]);

							//appending document list at the end of numDocsWithWord
				    	String finalVal = Integer.toString(combinedVal)+ "/";

				    	for(int i=1; i<firstValue.length; i++)
				    		finalVal = finalVal + firstValue[i] + ",";
				    	
				    	for(int i=1; i<secondValue.length-1; i++)
				    		finalVal = finalVal + secondValue[i] + ",";
				    	
				    	finalVal = finalVal + secondValue[secondValue.length-1];

				    	return (finalVal);
				    }
				}).flatMapToPair(
						new PairFlatMapFunction<Tuple2<String,String>,String,String>() {
							public Iterable<Tuple2<String,String>> call(Tuple2<String,String> x) {
								// Collect data attributes
								String[] arr2 = x._2.split("/");
								String[] docArr = arr2[1].split(",");
								String numDocsStr = String.valueOf(numDocs);
								
								// Output to Arraylist
								ArrayList ret = new ArrayList();
								for(String doc : docArr) {
									ret.add(new Tuple2(x._1+"@"+doc, numDocsStr+"/"+arr2[0]));									
								}

								return ret;
							}
				});
			
		//Print idfRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = idfRDD.collect();
			System.out.println("-------Contents of idfRDD-------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}
	
		/*
		 * TF * IDF Job
		 * Calculates final TFIDF value from tfRDD and idfRDD
		 *
		 * Input:  ( (word@document) , (wordCount/docSize) )          [from tfRDD]
		 * Map:    ( (word@document) , TF )
		 * 
		 * Input:  ( (word@document) , (numDocs/numDocsWithWord) )    [from idfRDD]
		 * Map:    ( (word@document) , IDF )
		 * 
		 * Union:  ( (word@document) , TF )  U  ( (word@document) , IDF )
		 * Reduce: ( (word@document) , TFIDF )
		 * Map:    ( (document@word) , TFIDF )
		 *
		 * where TF    = wordCount/docSize
		 * where IDF   = ln(numDocs/numDocsWithWord)
		 * where TFIDF = TF * IDF
		 */
		JavaPairRDD<String,Double> tfFinalRDD = tfRDD.mapToPair(
			new PairFunction<Tuple2<String,String>,String,Double>() {
				public Tuple2<String,Double> call(Tuple2<String,String> x) {
					double wordCount = Double.parseDouble(x._2.split("/")[0]);
				double docSize = Double.parseDouble(x._2.split("/")[1]);
					double TF = wordCount/docSize;
					return new Tuple2(x._1, TF);
				}
			}
		);
		
		JavaPairRDD<String,Double> idfFinalRDD = idfRDD.mapToPair(
		new PairFunction<Tuple2<String,String>,String,Double>() {
			public Tuple2<String,Double> call(Tuple2<String,String> x) {
				//calculating idf value
				double numDocs = Double.parseDouble(x._2.split("/")[0]);
				double numDocsWithWord = Double.parseDouble(x._2.split("/")[1]);
				double IDF = Math.log(numDocs/numDocsWithWord);
				return new Tuple2(x._1, IDF);
			}
		}
	);
		
		JavaPairRDD<String,Double> tfidfRDD = tfFinalRDD.union(idfFinalRDD).reduceByKey(new Function2<Double, Double, Double>() {
		    //@Override
		    public Double call(Double v1,Double v2) {	
					//Calculating TFIDF value
		    	double TFIDF=v1*v2;
		    	return (TFIDF);
		    }
		}).flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Double>,String,Double>() {
					public Iterable<Tuple2<String,Double>> call(Tuple2<String,Double> x) {
						// Collect data attributes
						String[] arr1 = x._1.split("@");

						// Output to Arraylist
						ArrayList ret = new ArrayList();
						ret.add(new Tuple2(arr1[1]+"@"+arr1[0], x._2));
						return ret;
					}
		});		
		
		//Print tfidfRDD contents in sorted order
		Map<String, Double> sortedMap = new TreeMap<>();
		List<Tuple2<String, Double>> list = tfidfRDD.collect();
		for (Tuple2<String, Double> tuple : list) {
			sortedMap.put(tuple._1, tuple._2);
		}
		if(DEBUG) System.out.println("-------Contents of tfidfRDD-------");
		for (String key : sortedMap.keySet()) {
			System.out.println(key + "\t" + sortedMap.get(key));
		}
		if(DEBUG) System.out.println("--------------------------------");	

		long endTime = System.currentTimeMillis();
			System.out.println("Total execution time: " + (endTime - startTime) ); 
	}	
}
