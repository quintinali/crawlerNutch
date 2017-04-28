package pd.nutch.ranking.pagerank;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.graphframes.lib.PageRank;
import org.graphframes.GraphFrame;
import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerAbstract;
import pd.nutch.main.CrawlerConstants;
import pd.nutch.main.CrawlerEngine;
import scala.Tuple2;

public class RankGraphFrame extends CrawlerAbstract {

	public RankGraphFrame(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
		// TODO Auto-generated constructor stub
	}

	public JavaPairRDD<String, Double> rank(String nodeFile, String edgeFile, String outFile) {

		JavaRDD<String> nodeStringRDD = spark.sc.textFile(nodeFile).distinct();
		JavaRDD<Node> nodeRDD = nodeStringRDD.map(f -> new Node(f));
		Dataset<Row> verDF = spark.sqlContext.createDataFrame(nodeRDD, Node.class);

		JavaRDD<String> edgeStringRDD = spark.sc.textFile(edgeFile).distinct();
		JavaRDD<Relations> relationRDD = edgeStringRDD.map(f -> {
			String[] parts = f.split(" ");
			return new Relations(parts[0], parts[1], "linkto");
		});
		Dataset<Row> edgDF = spark.sqlContext.createDataFrame(relationRDD, Relations.class);

		// Create a GraphFrame
		GraphFrame gFrame = new GraphFrame(verDF, edgDF);
		// Get in-degree of each vertex.
		// gFrame.inDegrees().show();
		// Count the number of "follow" connections in the graph.
		long count = gFrame.edges().filter("relationship = 'linkto'").count();
		// Run PageRank algorithm, and show results.
		PageRank pRank = gFrame.pageRank().resetProbability(0.01).maxIter(20);
		//pRank.run().vertices().select("id", "pagerank").show();
		Dataset<Row> nodeScore = pRank.run().vertices().select("id", "pagerank");
		//nodeScore
		
		JavaPairRDD<String, Double> ranks = nodeScore.toJavaRDD().mapToPair(new PairFunction<Row, String, Double>() {
			public Tuple2<String, Double> call(Row r) throws Exception {
				return new Tuple2<String, Double>((String)r.get(0), (Double)r.get(1));
			}
		});

		//for test
		ranks = ranks.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());
		try {
			this.createFile(outFile, ranks);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ranks;
	}
	
	private void createFile(String fileName, JavaPairRDD<String, Double> ranks) throws IOException {
		FileWriter fw = null;
		BufferedWriter bw = null;
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
		}
		try {
			file.createNewFile();
		} catch (IOException e2) {
			e2.printStackTrace();
		}

		try {
			fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<Tuple2<String, Double>> output = ranks.collect();
		for (Tuple2<?, ?> tuple : output) {
			// System.out.println(tuple._1() + " has rank: " + tuple._2() +
			// ".");
			bw.write(tuple._1() + " has rank: " + tuple._2() + "\n");
		}

		bw.close();
		fw.close();
	}

	public static void main(String[] args) {

		CrawlerEngine me = new CrawlerEngine();
		me.loadConfig();
		SparkDriver spark = new SparkDriver(me.getConfig());
		ESDriver es = new ESDriver(me.getConfig());
		
		RankGraphFrame frame = new RankGraphFrame(me.loadConfig(), es, spark);
		String path = me.loadConfig().getProperty(CrawlerConstants.FILE_PATH);
		//frame.rank("D:/crawlerData/node.txt", "D:/crawlerData/relation.txt", "D:/crawlerData/ranktest.txt");
		frame.rank(path + "pageUrls.txt", path + "pageRelations.txt", path + "ranktest.txt");
	}
}