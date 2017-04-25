package pd.nutch.ranking.webpage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.google.common.collect.Iterables;

import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerAbstract;
import pd.nutch.main.CrawlerConstants;
import scala.Tuple2;

public class PageRank extends CrawlerAbstract {
	
	String indexName;
	String typeName;
	
	public PageRank(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
		// TODO Auto-generated constructor stub
		indexName = props.getProperty(CrawlerConstants.ES_INDEX_NAME);
		typeName = props.getProperty(CrawlerConstants.CRAWLER_TYPE_NAME);
	}

	private static class Sum implements Function2<Double, Double, Double> {
		@Override
		public Double call(Double a, Double b) {
			return a + b;
		}
	}

	public JavaPairRDD<String, Double> rank(String inFileName, int iteration, String outFileName) throws Exception {
		// Loads in input file. It should be in format of:
		// URL neighbor URL
		// URL neighbor URL
		// URL neighbor URL
		// ...
		JavaRDD<String> lines = spark.sc.textFile(inFileName);

		// Loads all URLs from input file and initialize their neighbors.
		JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
			String[] parts = s.split(" ");
			return new Tuple2<>(parts[0], parts[1]);
		}).distinct().groupByKey().cache();

		System.out.println(links.count());

		// Loads all URLs with other URL(s) link to from input file and
		// initialize ranks of them to one.
		JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

		//System.out.println(ranks.count());

		// Calculates and updates URL ranks continuously using PageRank
		// algorithm.
		for (int current = 0; current < iteration; current++) {
			// Calculates URL contributions to the rank of other URLs.
			JavaRDD<Tuple2<Iterable<String>, Double>> test = links.join(ranks).values();
			JavaPairRDD<String, Double> contribs = test
					.flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
						@Override
						public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> arg0)
								throws Exception {
							// TODO Auto-generated method stub
							int urlCount = Iterables.size(arg0._1());
							List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
							for (String n : arg0._1) {
								results.add(new Tuple2<>(n, arg0._2() / urlCount));
							}
							return results;
						}
					});

			// Re-calculates URL ranks based on neighbor contributions.
			ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
		}

		//System.out.println(ranks.count());
		ranks = ranks.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());
		// Collects all URL ranks and dump them to console.

		createFile(outFileName, ranks);
		
		return ranks;
	}

	private static void createFile(String fileName, JavaPairRDD<String, Double> ranks) throws IOException {
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
}