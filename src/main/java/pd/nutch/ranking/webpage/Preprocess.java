package pd.nutch.ranking.webpage;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.elasticsearch.ElasticsearchException;
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

public class Preprocess extends CrawlerAbstract {

	String indexName;
	String typeName;

	public Preprocess(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
		// TODO Auto-generated constructor stub

		indexName = props.getProperty(CrawlerConstants.ES_INDEX_NAME);
		typeName = props.getProperty(CrawlerConstants.CRAWLER_TYPE_NAME);
	}

	public void convertDateToLong() {

		es.createBulkProcesser();
		SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(indexName).setTypes(typeName)
				.setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100);

		SearchResponse scrollResp = scrollBuilder.execute().actionGet();
		// last modified date 2017-04-24T22:10:27.370Z
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'");

		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> result = hit.getSource();
				String date = (String) result.get("date");

				Date dateopt = null;
				try {
					dateopt = (Date) formatter.parse(date);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				long datelong = dateopt.getTime();

				UpdateRequest ur = null;
				try {
					ur = new UpdateRequest(indexName, typeName, hit.getId())
							.doc(jsonBuilder().startObject().field("datelong", datelong).endObject());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				es.getBulkProcessor().add(ur);
			}

			scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
					.execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}

		es.destroyBulkProcessor();
		es.refreshIndex();
	}

	public void run() {
		this.convertDateToLong();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CrawlerEngine me = new CrawlerEngine();
		me.loadConfig();
		SparkDriver spark = new SparkDriver(me.getConfig());
		ESDriver es = new ESDriver(me.getConfig());

		Preprocess lda = new Preprocess(me.loadConfig(), es, spark);
		lda.run();
	}
}
