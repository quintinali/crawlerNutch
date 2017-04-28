package pd.nutch.ranking.webpage;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import breeze.linalg.DenseVector;
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
	transient SparkContext sc = null;

	int topicNum = 15;
	String path = props.getProperty(CrawlerConstants.FILE_PATH);

	public LocalLDAModel trainedLdaModel = null;
	Map<String, Integer> wordindexMap = new HashMap<String, Integer>();

	public LDAAnalysis(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
		// TODO Auto-generated constructor stub
		indexName = props.getProperty(CrawlerConstants.ES_INDEX_NAME);
		typeName = props.getProperty(CrawlerConstants.CRAWLER_TYPE_NAME);

		sc = spark.sc.sc();
		trainedLdaModel = this.loadModel();
	}

	public void exportPageContent(String fileName)
			throws InterruptedException, ExecutionException, ElasticsearchException, IOException {

		FileWriter fw = this.createFile(fileName);
		BufferedWriter bw = new BufferedWriter(fw);

		SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(indexName).setTypes(typeName)
				.setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100);
		int docnum = es.getDocCount(indexName, QueryBuilders.matchAllQuery(), typeName);
		SearchResponse scrollResp = scrollBuilder.execute().actionGet();
		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> result = hit.getSource();
				String lang = (String) result.get("lang");
				if (lang == null || !lang.equals("en")) {
					continue;
				}
				String id = (String) result.get("url");
				String title = (String) result.get("content");
				title = title.replaceAll("\\d", "");
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

		bw.close();
		fw.close();
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

		// save model and words
		ldaModel.toLocal().save(spark.sc.sc(), path + "LDAModel");
		saveWords(wordIndexRDD);

		// predict word topic
		predictWordTopic(wordIndexRDD);
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
						UpdateRequest ur = new UpdateRequest(indexName, typeName, url)
								.doc(jsonBuilder().startObject().field("topicProps", propArray).endObject());
						tmpES.getBulkProcessor().add(ur);

						realUserNums.add(1);
					}
					tmpES.destroyBulkProcessor();
					tmpES.close();
					return realUserNums.iterator();
				}).reduce((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

		System.out.println("doc count: " + Integer.toString(userCount));
	}

	private void predictWordTopic() throws IOException {

		String wordindexType = props.getProperty(CrawlerConstants.WORD_INDEX_TYPE);
		SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(indexName).setTypes(wordindexType)
				.setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100);

		List<Tuple2<String, Long>> wordIndexList = new ArrayList<Tuple2<String, Long>>();
		SearchResponse scrollResp = scrollBuilder.execute().actionGet();
		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> result = hit.getSource();
				String word = (String) result.get("word");
				int index = (int) result.get("index");
				wordIndexList.add(new Tuple2<String, Long>(word, (long) index));
			}

			scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
					.execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}

		JavaPairRDD<String, Long> wordRDD = spark.sc.parallelizePairs(wordIndexList, 64);
		this.predictWordTopic(wordRDD);
	}

	private void predictWordTopic(JavaPairRDD<String, Long> wordRDD) throws IOException {

		String wordtopicType = props.getProperty(CrawlerConstants.WORD_TOPIC_TYPE);
		es.deleteType(indexName, wordtopicType);

		int wordNum = (int) wordRDD.count();
		LocalLDAModel model = this.loadModel();
		Broadcast<LocalLDAModel> broadmodels = spark.sc.broadcast(model);
		Broadcast<Integer> broadWordNum = spark.sc.broadcast(wordNum);
		int wordCount = 0;
		wordCount = wordRDD.mapPartitions((FlatMapFunction<Iterator<Tuple2<String, Long>>, Integer>) iterator -> {
			// TODO Auto-generated method stub
			ESDriver tmpES = new ESDriver(props);
			tmpES.createBulkProcesser();
			List<Integer> realwordNums = new ArrayList<Integer>();
			Integer wordnum = broadWordNum.getValue();
			LocalLDAModel localmodel = broadmodels.getValue();
			while (iterator.hasNext()) {
				Tuple2<String, Long> wordIndex = iterator.next();
				String word = wordIndex._1();
				int[] pos = new int[1];
				double[] value = new double[1];
				pos[0] = wordIndex._2().intValue();
				value[0] = 1.0;

				Vector wordvec = Vectors.sparse(wordnum, pos, value);
				/// System.out.println(ArrayUtils.toString(wordvec.toArray()));
				double[] topicArray = localmodel.topicDistribution(wordvec).toArray();
				// System.out.println(ArrayUtils.toString(topicArray));
				IndexRequest ir = new IndexRequest(indexName, wordtopicType).source(
						jsonBuilder().startObject().field("word", word).field("topicProps", topicArray).endObject());
				tmpES.getBulkProcessor().add(ir);

				realwordNums.add(1);
			}
			tmpES.refreshIndex();
			tmpES.destroyBulkProcessor();
			tmpES.close();
			return realwordNums.iterator();
		}).reduce((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

		System.out.println("word count: " + Integer.toString(wordCount));
	}

	private void saveWords(JavaPairRDD<String, Long> wordIndexRDD) throws IOException {

		String wordindexType = props.getProperty(CrawlerConstants.WORD_INDEX_TYPE);
		es.deleteType(indexName, wordindexType);

		int wordCount = 0;
		wordCount = wordIndexRDD.mapPartitions((FlatMapFunction<Iterator<Tuple2<String, Long>>, Integer>) iterator -> {
			// TODO Auto-generated method stub
			ESDriver tmpES = new ESDriver(props);
			tmpES.createBulkProcesser();
			List<Integer> realwordNums = new ArrayList<Integer>();
			while (iterator.hasNext()) {
				Tuple2<String, Long> s = iterator.next();
				String word = s._1();
				Long index = s._2();

				IndexRequest ir = new IndexRequest(indexName, wordindexType)
						.source(jsonBuilder().startObject().field("word", word).field("index", index).endObject());
				tmpES.getBulkProcessor().add(ir);

				realwordNums.add(1);
			}
			tmpES.destroyBulkProcessor();
			tmpES.close();
			return realwordNums.iterator();
		}).reduce((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

		System.out.println("word count: " + Integer.toString(wordCount));
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

	public double[] predict(/* LocalLDAModel trainedLdaModel, */String words) {
		Vector wordVec = null;
		try {
			wordVec = this.getWordVec(es, words);
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Vector topic = trainedLdaModel.topicDistribution(wordVec);
		System.out.println(topic.toString());
		return topic.toArray();
	}

	protected Vector getWordVec(ESDriver es, String words) throws InterruptedException, ExecutionException {

		String[] wordArray = es.customAnalyzing("parse", words).split(" ");
		Map<String, Double> wordMap = new HashMap<String, Double>();
		for (String word : wordArray) {
			if (wordMap.containsKey(word)) {
				Double count = wordMap.get(word);
				wordMap.put(word, count + 1.0);
			} else {
				wordMap.put(word, 1.0);
			}
		}
		int[] pos = new int[wordMap.size()];
		double[] value = new double[wordMap.size()];

		String wordindexType = props.getProperty(CrawlerConstants.WORD_INDEX_TYPE);
		int wordNum = es.getDocCount(indexName, QueryBuilders.matchAllQuery(), wordindexType);

		BoolQueryBuilder qb = new BoolQueryBuilder();
		for (Entry<String, Double> entry : wordMap.entrySet()) {
			qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), "word").boost(entry.getValue().floatValue())
					.operator(MatchQueryBuilder.Operator.OR).tieBreaker((float) 0.5));
		}

		SearchRequestBuilder searchBuilder = es.getClient().prepareSearch(indexName).setTypes(wordindexType)
				.setQuery(qb).setSize(wordMap.size());
		SearchResponse response = searchBuilder.execute().actionGet();
		int i = 0;
		for (SearchHit hit : response.getHits().getHits()) {
			Map<String, Object> result = hit.getSource();
			String word = (String) result.get("word");
			int index = (int) result.get("index");

			// System.out.println(word);
			if (wordMap.containsKey(word)) {
				pos[i] = index;
				value[i] = wordMap.get(word);
			}

			i += 1;
		}

		// System.out.println(ArrayUtils.toString(pos));
		Vector wordVec = Vectors.sparse(wordNum, pos, value);
		return wordVec;
	}

	public LocalLDAModel loadModel() {
		String path = props.getProperty(CrawlerConstants.FILE_PATH);
		Path ldaPath = Paths.get(path + "LDAModel");
		if (!Files.exists(ldaPath)) {
			return null;
		}

		trainedLdaModel = LocalLDAModel.load(sc, path + "LDAModel");

		return trainedLdaModel;
	}

	public void run() {
		String path = props.getProperty(CrawlerConstants.FILE_PATH);
		try {
			exportPageContent(path + "pageContents.txt");
			LDAAnalysis(path + "pageContents.txt");
		} catch (ElasticsearchException | InterruptedException | ExecutionException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// for test
	public void printModel(LocalLDAModel ldaModel) {

		Map<Integer, String> tokenIds = new HashMap<Integer, String>();
		String wordindexType = props.getProperty(CrawlerConstants.WORD_INDEX_TYPE);
		SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(indexName).setTypes(wordindexType)
				.setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100)
				.addSort("index", SortOrder.ASC);
		SearchResponse scrollResp = scrollBuilder.execute().actionGet();

		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> result = hit.getSource();
				String word = (String) result.get("word");
				int index = (int) result.get("index");
				tokenIds.put(index, word);
			}

			scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
					.execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}

		System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize() + " words):");

		Tuple2<int[], double[]>[] topics = ldaModel.describeTopics(100);
		for (int topic = 0; topic < topics.length; topic++) {
			System.out.print("Topic " + topic + ":");
			int[] word = topics[topic]._1;
			double[] weight = topics[topic]._2;
			for (int i = 0; i < word.length; i++) {
				System.out.print(
						"  " + tokenIds.get(word[i]) /* + ":" + weight[i] */);
			}

			System.out.println();
		}
	}

	private void printGoldWordTopic() {
		LocalLDAModel model = this.loadModel();
		String path = props.getProperty(CrawlerConstants.FILE_PATH);
		JavaRDD<String> vocabRDD = spark.sc.textFile(path + "goldstandard.txt");
		List<String> vocabularies = vocabRDD.map(f -> f.split(",")[0]).collect();
		for (int i = 0; i < vocabularies.size(); i++) {
			System.out.println(vocabularies.get(i));
			this.predict(vocabularies.get(i));
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CrawlerEngine me = new CrawlerEngine();
		me.loadConfig();
		SparkDriver spark = new SparkDriver(me.getConfig());
		ESDriver es = new ESDriver(me.getConfig());

		String str = "Scientists Eager for Hubble's Facelift Science & Astronomy Scientists Eager for Hubble's Facelift by Clara Moskowitz, SPACE.com Assistant Managing Editor | July 1, 2008 05:05pm ET NASA's Hubble Space Telescope maintains its orbit around Earth. The space agency hopes to upgrade the aging observatory some time in August 2008. Original Image Credit: NASA Scientists are eagerly awaiting a much-needed facelift planned for the world?s favorite space telescope. This fall NASA astronauts plan to take a final space shuttle trip to fix the aging Hubble Space Telescope (HST). Set to fly Oct. 8 from NASA's Kennedy Space Center (KSC) in Cape Canaveral, Fla., the mission will carry seven astronauts aboard the shuttle Atlantis to upgrade the 18-year-old Hubble. Ground crews at KSC are hard at work on repairs to Launch Pad 39A, which suffered damage during the last shuttle liftoff of Discovery on May 31. The work is expected to be finished in time for Atlantis? launch. ?Hubble?s been flying for over 18 years, and although it?s old, there?s still a lot of great science left in this telescope,? said Preston Burch, HST program manager, at a briefing Tuesday at NASA?s Goddard Spaceflight Center. ?We believe that [this mission] is going to enable us to finally unleash the full potential of the Hubble Space Telescope.? The STS-125 mission will be the fifth trip to repair the orbiting telescope, which has been circling earth about every 97 minutes since it launched in April 1990 . The planned 11-day mission is slated to install new equipment and repair broken instruments during five spacewalks. New additions Atlantis is scheduled to deliver the Wide Field Camera 3, which was designed to image the distant universe in a broad range of wavelengths, from near ultraviolet light through optical light and into the near infrared. It will be particularly adept at studying some of the oldest, most distant galaxies in the universe, whose light has been redshifted to the infrared range. ?We have no idea what the universe looks like at these very high redshifts,? said Matt Mountain, director of the Space Telescope Science Institute. ?Our first hint will come from Wide Field Camera 3.? The mission is also due to bring Hubble the Cosmic Origins Spectrograph (COS), an instrument that can break up light into its constituent colors to reveal the chemical makeup and other fundamental properties of heavenly objects. In addition to the new scientific instruments, Atlantis is set to deliver a set of six new and improved gyroscopes, which help stabilize the telescope, to replace its old six, three of which are broken. The shuttle mission is also slated to repair some broken instruments aboard the observatory, and bring new batteries and thermal blankets that should help the telescope operate until at least 2013. The crew is also planning to install a docking port called a Soft Capture Mechanism to the observatory. When the telescope is ready to be retired, a future unmanned spacecraft could attach to this device to steer Hubble on a controlled dive down to Earth and to its demise. New and improved NASA hopes the upcoming upgrades will help Hubble have a healthy life for a while yet. To that end, they?ve planned a packed mission to leave the telescope in the best shape possible. The numerous activities scheduled for the five busy spacewalking days will be a challenging undertaking, mission managers said. ?Even if we just get one day [of spacewalking repairs] in, we?d have a much better telescope than we have now,? said Keith Walyus, Hubble Space Telescope Servicing Mission Operations Manager. ?If we get all this done ? wow, that?s going to be absolutely amazing.? The mission has had a rocky history. Originally cancelled in the wake of the Columbia disaster in 2003, for a while NASA deemed it too risky and expensive a venture. NASA considered sending a robotic repair mission to Hubble instead, but eventually decided a manned mission was the only way to accomplish what needed to be done. ?The technology they were looking at is amazing,? Walyus said of the proposed robotic fixes, ?but it?s just not the same as a human. This was built to be worked on by humans.? Ultimately the strong public and political support for the mission helped influence NASA to decide to return to the space telescope one last time. ?The American people stood up and said wait a minute, not so fast, this is our telescope,? said David Leckrone, Hubble senior project scientist at Goddard, of the response to the mission?s cancellation. Beloved by many The telescope achieves its amazing feats by orbiting 360 miles (575 kilometers) above the surface of the Earth, where it bypasses our planet's thick atmosphere, which blocks out light and distorts the view from space akin to looking at trees from the bottom of a swimming pool. While Hubble's 2.4 meter (94.5 inch)-wide primary mirror would be considered dinky compared to the largest ground-based observatories (the W. M. Keck Observatory in Maua Kea, Hawaii has two scopes with 10-meter, or 400-inch, primary mirrors), in space it is enough to observe the distant cosmos in unmatched detail. These visions have produced not only groundbreaking scientific discoveries, but also unprecedented enthusiasm from non-scientists. ?When the public saw for the first time the absolute stunning beauty of the universe we live in, that was a major shift in the way people looked at the world,? Leckrone said. ?I think there?s a big element of pride on the part of the American people in having produced Hubble and used it in this way. I think as a species we all take collective pride in, hey, this is something we did, and it was something very hard to do.? NEW GALLERY: Hubble Photos: When Galaxies Collide NEW VIDEO: Space Shuttle Bloopers Video: Hubble Repair Missions   Loading ... Author Bio Clara Moskowitz, SPACE.com Assistant Managing Editor Clara has been SPACE.com's Assistant Managing Editor since 2011, and has been writing for SPACE.com and LiveScience since 2008. Clara has a bachelor's degree in astronomy and physics from Wesleyan University, and a graduate certificate in science writing from the University of California, Santa Cruz. To find out what her latest project is, you can follow Clara on Google+ . Clara Moskowitz, SPACE.com Assistant Managing Editor on FOLLOW US Copyright © 2017 All Rights Reserved.";

		try {
			System.out.println(es.customAnalyzing("parse", str));
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		LDAAnalysis lda = new LDAAnalysis(me.loadConfig(), es, spark);
		/*
		 * lda.run(); LocalLDAModel model = lda.loadModel();
		 * lda.predict(model,"neo"); lda.predict(model,"near earth object");
		 */

		// print model
		/*
		 * LocalLDAModel model = lda.loadModel(); lda.printModel(model);
		 */

	}
}