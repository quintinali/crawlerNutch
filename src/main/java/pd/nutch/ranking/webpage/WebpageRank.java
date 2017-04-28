package pd.nutch.ranking.webpage;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerAbstract;
import pd.nutch.main.CrawlerConstants;
import pd.nutch.main.CrawlerEngine;
import pd.nutch.ranking.pagerank.RankGraphFrame;
import scala.Tuple2;

public class WebpageRank extends CrawlerAbstract {
	String indexName;
	String typeName;

	public WebpageRank(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
		// TODO Auto-generated constructor stub
		indexName = props.getProperty(CrawlerConstants.ES_INDEX_NAME);
		typeName = props.getProperty(CrawlerConstants.CRAWLER_TYPE_NAME);
	}

	public void rankAndupdate(String urlFile, String relationFile, String outFileName) throws Exception {
		RankGraphFrame frame = new RankGraphFrame(props, es, spark);
		JavaPairRDD<String, Double> ranks = frame.rank(urlFile, relationFile, outFileName);
		int pageCount = 0;
		pageCount = ranks.mapPartitions((FlatMapFunction<Iterator<Tuple2<String, Double>>, Integer>) iterator -> {
			// TODO Auto-generated method stub
			ESDriver tmpES = new ESDriver(props);
			tmpES.createBulkProcesser();
			List<Integer> realUserNums = new ArrayList<Integer>();
			while (iterator.hasNext()) {
				Tuple2<String, Double> s = iterator.next();
				String url = s._1();
				Double webpageRank = s._2();
				UpdateRequest ur = new UpdateRequest(indexName, typeName, url)
						.doc(jsonBuilder().startObject().field("webpageRank", webpageRank).endObject());
				tmpES.getBulkProcessor().add(ur);
				realUserNums.add(1);
			}
			tmpES.destroyBulkProcessor();
			tmpES.close();
			return realUserNums.iterator();
		}).reduce((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

		System.out.println("page count" + pageCount);
	}

	public void exportWebPageUrlLinks(String urlFile, String relationFile) throws IOException {

		FileWriter urlfw = this.createFile(urlFile);
		BufferedWriter urlbw = new BufferedWriter(urlfw);

		FileWriter relationfw = this.createFile(relationFile);
		BufferedWriter relationbw = new BufferedWriter(relationfw);

		SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(indexName).setTypes(typeName)
				.setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100);

		int docnum = es.getDocCount(indexName, QueryBuilders.matchAllQuery(), typeName);
		SearchResponse scrollResp = scrollBuilder.execute().actionGet();

		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> result = hit.getSource();
				String url = (String) result.get("url");
				urlbw.write(url + "\n");
				String inlinks = (String) result.get("url_inlinks");
				String outlinks = (String) result.get("url_outlinks");
				if (inlinks != null && !inlinks.isEmpty()) {
					String[] inlinkArray = inlinks.split("&&&&");
					for (String inlink : inlinkArray) {
						if (!inlink.trim().isEmpty()) {
							relationbw.write(inlink + " " + url + "\n");
							urlbw.write(inlink + "\n");
						}
					}
				}

				if (outlinks != null && !outlinks.isEmpty()) {
					String[] outlinkArray = outlinks.split("&&&&");
					for (String outlink : outlinkArray) {
						if (!outlink.trim().isEmpty()) {
							relationbw.write(url + " " + outlink + "\n");
							urlbw.write(outlink + "\n");
						}
					}
				}
			}

			scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
					.execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}

		relationbw.close();
		relationfw.close();
		urlbw.close();
		urlfw.close();
	}
	
	public void run(){
		String path = props.getProperty(CrawlerConstants.FILE_PATH);
		try {
			String urlFile = path + "pageUrls.txt";
			String urlRelationFile = path + "pageRelations.txt";
			String pagerankFile = path + "webpageRanks.txt";
			exportWebPageUrlLinks(urlFile, urlRelationFile);
			rankAndupdate(urlFile, urlRelationFile, pagerankFile);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CrawlerEngine me = new CrawlerEngine();
		me.loadConfig();
		SparkDriver spark = new SparkDriver(me.getConfig());
		ESDriver es = new ESDriver(me.getConfig());

		WebpageRank webpagerank = new WebpageRank(me.loadConfig(), es, spark);
		webpagerank.run();
	}
}
