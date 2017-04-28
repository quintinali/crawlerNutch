package pd.nutch.crawler;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.lucene.search.Explanation;
import org.apache.spark.api.java.JavaRDD;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerAbstract;
import pd.nutch.main.CrawlerConstants;
import pd.nutch.main.CrawlerEngine;
import pd.nutch.ranking.webpage.LDAAnalysis;

public class GoldKeywordExtractor extends CrawlerAbstract {

	String indexName;
	String typeName;

	public GoldKeywordExtractor(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
		// TODO Auto-generated constructor stub
		indexName = props.getProperty(CrawlerConstants.ES_INDEX_NAME);
		typeName = props.getProperty(CrawlerConstants.CRAWLER_TYPE_NAME);
	}

	public void extractGoldenKeyWords() {

		es.createBulkProcesser();

		BoolQueryBuilder bq = this.createVocabularyQuery();
		SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(indexName).setTypes(typeName)
				.setScroll(new TimeValue(60000)).setQuery(bq).setSize(100).setExplain(true)
				.setFetchSource(new String[] { "_explanation" }, null);

		//System.out.println(scrollBuilder.toString());
		SearchResponse scrollResp = scrollBuilder.execute().actionGet();

		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> metadata = hit.getSource();

				List<String> goldenKeywordList = new ArrayList<String>();
				Explanation totalexplain = hit.getExplanation();
				Explanation[] all_explains = totalexplain.getDetails();
				for (Explanation explain : all_explains) {
					Explanation[] nested_level1_explains = explain.getDetails();

					if (nested_level1_explains == null) {
						continue;
					}

					for (Explanation nested_level1_explain : nested_level1_explains) {
						Explanation[] subexplains = nested_level1_explain.getDetails();
						if (subexplains != null && subexplains.length > 0) {
							for (Explanation nestedexplain : subexplains) {
								Explanation[] finalexplains = nestedexplain.getDetails();
								for (Explanation finalexplain : finalexplains) {
									String description = finalexplain.getDescription();
									description = description.replaceAll("weight\\(content\\:", "");
									int pos = description.indexOf(" in ");
									if (pos >= 0) {
										String keyword = description.substring(0, pos);
										if (keyword.startsWith("\"")) {
											keyword = keyword.substring(1, keyword.length() - 1);
										}
										goldenKeywordList.add(keyword);
									}
								}
							}
						}
					}
				}

				List<String> deduped = goldenKeywordList.stream().distinct().collect(Collectors.toList());
				String goldKeywords = String.join(",", deduped);
				System.out.println(goldKeywords);

				UpdateRequest ur = null;
				try {
					ur = new UpdateRequest(indexName, typeName, hit.getId())
							.doc(jsonBuilder().startObject().field("gold_keywords", goldKeywords).endObject());
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

	}

	private BoolQueryBuilder createVocabularyQuery() {

		String path = props.getProperty(CrawlerConstants.FILE_PATH);
		JavaRDD<String> vocabRDD = spark.sc.textFile(path + "goldstandard.txt");
		List<String> vocabularies = vocabRDD.map(f -> f.split(",")[0]).collect();

		BoolQueryBuilder qb = new BoolQueryBuilder();
		String fieldsList[] = { "Title", "content" };

		int length = vocabularies.size();
		for (int i = 0; i < length; i++) {
			String vob = vocabularies.get(i).toLowerCase();
			qb.should(QueryBuilders.multiMatchQuery(vob, fieldsList).type(MultiMatchQueryBuilder.Type.PHRASE)
					.tieBreaker((float) 0.5));
		}

		return qb;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CrawlerEngine me = new CrawlerEngine();
		me.loadConfig();
		SparkDriver spark = new SparkDriver(me.getConfig());
		ESDriver es = new ESDriver(me.getConfig());

		GoldKeywordExtractor gold = new GoldKeywordExtractor(me.loadConfig(), es, spark);
		gold.extractGoldenKeyWords();
	}
}
