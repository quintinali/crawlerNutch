package pd.nutch.ranking.webpage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import com.google.common.base.Optional;
import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerAbstract;
import pd.nutch.main.CrawlerConstants;
import pd.nutch.main.CrawlerEngine;
import pd.nutch.structure.LabeledRowMatrix;
import pd.nutch.structure.MatrixUtil;
import scala.Tuple2;
import scala.Tuple3;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class LDAAnalysis extends CrawlerAbstract {
	String indexName;
	String typeName;

	int topicNum = 10;
	String path = props.getProperty(CrawlerConstants.FILE_PATH);

	public LDAAnalysis(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
		// TODO Auto-generated constructor stub
		indexName = props.getProperty(CrawlerConstants.ES_INDEX_NAME);
		typeName = props.getProperty(CrawlerConstants.CRAWLER_TYPE_NAME);
	}

	public void getPageContent(String fileName)
			throws InterruptedException, ExecutionException, ElasticsearchException, IOException {

		this.createFile(fileName);
		SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(indexName).setTypes(typeName)
				.setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100);
		int docnum = es.getDocCount(indexName, QueryBuilders.matchAllQuery(), typeName);
		SearchResponse scrollResp = scrollBuilder.execute().actionGet();
		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> result = hit.getSource();
				String lang = (String) result.get("lang");
				if (lang != null && !lang.equals("en")) {
					continue;
				}
				String id = (String) result.get("url");
				String title = (String) result.get("content");
				if (title != null) {
					title = title.replace(",", " ");
					title = title.replace("-", " ");
					title = title.replace(":", " ");
					title = es.customAnalyzing("parse", title);
				}

				try {
					bw.write(id + " " + title + "\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
					.execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}
	}

	public void LDAAnalysis(String filename) throws Exception {

		// prepare data
		JavaPairRDD<String, List<String>> datasetTokensRDD = this.loadData(filename);
		LabeledRowMatrix labelMatrix = MatrixUtil.createDocWordMatrix(datasetTokensRDD, spark.sc);
		JavaRDD<Vector> parsedData = labelMatrix.rowMatrix.rows().toJavaRDD();
		JavaRDD<String> row = labelMatrix.rowkeys;
		JavaRDD<String> word = labelMatrix.colkeys;

		// Index documents with unique IDs
		JavaPairRDD<Long, Vector> corpus = JavaPairRDD
				.fromJavaRDD(parsedData.zipWithIndex().map(new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
					public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
						return doc_id.swap();
					}
				}));
		corpus.cache();
		JavaPairRDD<String, Long> rowIndexRDD = row.zipWithIndex();
		JavaPairRDD<String, Long> wordIndexRDD = word.zipWithIndex();

		// Cluster the documents into three topics using LDA
		DistributedLDAModel ldaModel = (DistributedLDAModel) new LDA().setK(topicNum).setDocConcentration(2)
				.setTopicConcentration(2).setMaxIterations(50).setSeed(0L).setCheckpointInterval(10).setOptimizer("em")
				.run(corpus);

		// update document topics
		JavaPairRDD<Long, Vector> docTopicRDD = ldaModel.javaTopicDistributions();
		updateDocTopics(rowIndexRDD, docTopicRDD);

		//update word topic
		Tuple2<int[], double[]>[] topicDesces = ldaModel.describeTopics();
		this.updateWordTopic(word, topicDesces);
		//Matrix topicDesces = ldaModel.topicsMatrix();
		//this.updateWordTopic(word, topicDesces);

		// save model
		ldaModel.save(spark.sc.toSparkContext(spark.sc), path + "LDAModel");

		// save words
		wordIndexRDD.saveAsTextFile(path + "word.txt");

		// 
		// ldaModel.topTopicsPerDocument
		// JavaPairRDD<String, Long> words = labelMatrix.rowkeys.zipWithIndex();
		// Output topics. Each is a distribution over words (matching word
		/*
		 * JavaRDD<String> tokensRDD = datasetTokensRDD.values().flatMap(new
		 * FlatMapFunction<List<String>, String>() { public Iterable<String>
		 * call(List<String> list) { return list; } }).distinct(); List<String>
		 * allTokens = tokensRDD.collect(); Map<Integer, String> tokenIds = new
		 * HashMap<Integer, String>(); int length = allTokens.size(); for (int i
		 * = 0; i < length; i++) { tokenIds.put(i, allTokens.get(i)); }
		 * 
		 * System.out.println("Learned topics (as distributions over vocab of "
		 * + ldaModel.vocabSize() + " words):");
		 * 
		 * Tuple2<int[], double[]>[] topics = ldaModel.describeTopics(10); for
		 * (int topic = 0; topic < topics.length; topic++) {
		 * System.out.print("Topic " + topic + ":"); int[] word =
		 * topics[topic]._1; double[] weight = topics[topic]._2; for (int i = 0;
		 * i < word.length; i++) { System.out.print(" " + tokenIds.get(word[i])
		 * + ":" + weight[i]); }
		 * 
		 * System.out.println(); }
		 */
	}

	private void updateDocTopics(JavaPairRDD<String, Long> rowIndexRDD, JavaPairRDD<Long, Vector> docTopicRDD)
			throws InterruptedException, IOException {

		JavaPairRDD<Long, String> idDocRDD = rowIndexRDD
				.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
					@Override
					public Tuple2<Long, String> call(Tuple2<String, Long> arg0) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Long, String>(arg0._2, arg0._1);
					}
				});

		JavaPairRDD<String, double[]> urlTopicRDD = idDocRDD.leftOuterJoin(docTopicRDD)
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Vector>>>, String, double[]>() {
					@Override
					public Tuple2<String, double[]> call(Tuple2<Long, Tuple2<String, Optional<Vector>>> arg0)
							throws Exception {
						Tuple2<String, Optional<Vector>> test = arg0._2;
						Optional<Vector> optStr = test._2;
						double[] item = new double[5];
						if (optStr.isPresent()) {
							item = optStr.get().toArray();
						}
						return new Tuple2<>(test._1, item);
					}
				});

		int userCount = 0;
		userCount = urlTopicRDD
				.mapPartitions((FlatMapFunction<Iterator<Tuple2<String, double[]>>, Integer>) iterator -> {
					// TODO Auto-generated method stub
					ESDriver tmpES = new ESDriver(props);
					tmpES.createBulkProcesser();
					List<Integer> realUserNums = new ArrayList<Integer>();
					while (iterator.hasNext()) {
						Tuple2<String, double[]> s = iterator.next();
						String url = s._1();
						double[] propArray = s._2();
						UpdateRequest ur = new UpdateRequest(indexName, typeName, url).doc(jsonBuilder().startObject().field("topicProps", propArray).endObject());
						tmpES.getBulkProcessor().add(ur);
						System.out.println(ur.toString());
						
						realUserNums.add(1);
					}
					tmpES.destroyBulkProcessor();
					tmpES.close();
					return realUserNums;
				}).reduce((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

		System.out.println("doc count: " + Integer.toString(userCount));
	}

	/*private void updateWordTopic(JavaRDD<String> wordRDD, Matrix topicMatrix) throws IOException {

		String wordtopicType = props.getProperty(CrawlerConstants.WORD_TOPIC_TYPE);
		es.deleteType(indexName, wordtopicType);

		es.createBulkProcesser();

		double[] array = topicMatrix.toArray();
		List<String> words = wordRDD.collect();
		int wordNum = words.size();
		System.out.println("word num" + wordNum);

		for (int i = 0; i < wordNum; i++) {
			String word = words.get(i);
			int startPos = i * topicNum;
			double[] topicArray = ArrayUtils.subarray(array, startPos, startPos + topicNum);

			//System.out.println(word + ArrayUtils.toString(topicArray));
			
			double total = 0.0;
			for(int j=0; j<topicArray.length; j++){
				total += topicArray[j];
			}
			
			System.out.println(word + ":" + total);

			IndexRequest ir = new IndexRequest(indexName, wordtopicType).source(jsonBuilder().startObject()
					.field("word",word)
					.field("topicProps", topicArray).endObject());
			es.getBulkProcessor().add(ir);
		}

		es.destroyBulkProcessor();
	}*/
	
	private void updateWordTopic(JavaRDD<String> wordRDD, Tuple2<int[], double[]>[] topicDesces) throws IOException {

		String wordtopicType = props.getProperty(CrawlerConstants.WORD_TOPIC_TYPE);
		es.deleteType(indexName, wordtopicType);

		es.createBulkProcesser();
		int topicCount = topicDesces.length;
		List<String> words = wordRDD.collect();
		int count = words.size();
		Map<String, double[]> wordProps = new HashMap<String, double[]>();
		for(int i=0; i<count; i++){
			wordProps.put(words.get(i), new double[topicNum]);
		}
		
	    for( int t=0; t<topicCount; t++ ){
	        Tuple2<int[], double[]> topic = topicDesces[t];
	        int[] indices = topic._1();
	        double[] values = topic._2();
	        
	        int wordCount = indices.length;
	        for( int w=0; w<wordCount; w++ ){

	        	String word = words.get(w);
	            double prob = values[w];
	            
	            double[] oldProbs = wordProps.get(word);
	            oldProbs[t] = prob;
	            wordProps.put(word, oldProbs);
	        }
		}
	    
	    for(String word : wordProps.keySet() ){
	    	double[] topicArray = wordProps.get(word);
	    	double total = 0.0;
			for(int j=0; j<topicArray.length; j++){
				total += topicArray[j];
			}
			
			//System.out.println(word + ":" + ArrayUtils.toString(topicArray));
			IndexRequest ir = new IndexRequest(indexName, wordtopicType).source(jsonBuilder().startObject()
					.field("word",word)
					.field("topicProps", topicArray).endObject());
			es.getBulkProcessor().add(ir);
	    }

		es.destroyBulkProcessor();
	}

	public JavaPairRDD<String, List<String>> loadData(String filename) throws Exception {
		JavaRDD<String> data = spark.sc.textFile(filename);

		JavaPairRDD<String, List<String>> datasetsTokenPairRDD = data
				.mapToPair(new PairFunction<String, String, List<String>>() {
					public Tuple2<String, List<String>> call(String s) throws Exception {

						String[] sarray = s.trim().split(" ");
						String docId = sarray[0];
						List<String> words = new ArrayList<String>();
						for (int i = 1; i < sarray.length; i++) {
							words.add(sarray[i]);
						}
						return new Tuple2<String, List<String>>(docId, words);
					}
				});

		return datasetsTokenPairRDD;
	}

	public void predict(String words) {
		String path = props.getProperty(CrawlerConstants.FILE_PATH);
		DistributedLDAModel sameModel = DistributedLDAModel.load(spark.sc.toSparkContext(spark.sc), path + "LDAModel");

		// sameModel
		JavaPairRDD<String, String> pairRDD = JavaPairRDD.fromJavaRDD(spark.sc.objectFile(path + "word.txt"));
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CrawlerEngine me = new CrawlerEngine();
		me.loadConfig();
		SparkDriver spark = new SparkDriver(me.getConfig());
		ESDriver es = new ESDriver(me.getConfig());

		LDAAnalysis lda = new LDAAnalysis(me.loadConfig(), es, spark);
		String path = me.loadConfig().getProperty(CrawlerConstants.FILE_PATH);
		try {
			lda.getPageContent(path + "pageTitles.txt");
			lda.LDAAnalysis(path + "pageTitles.txt");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}