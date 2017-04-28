/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pd.nutch.ranking;

import java.io.PrintWriter;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.highlight.HighlightField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerAbstract;
import pd.nutch.main.CrawlerConstants;
import pd.nutch.main.CrawlerEngine;
import pd.nutch.ranking.webpage.LDAAnalysis;

/**
 * Supports ability to performance semantic search with a given query
 */
public class Searcher extends CrawlerAbstract implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
	DecimalFormat NDForm = new DecimalFormat("#.##");
	final Integer MAX_CHAR = 700;

	LDAAnalysis lda = null;

	public Searcher(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);

		lda = new LDAAnalysis(props, es, spark);
	}

	/**
	 * Method of checking if query exists in a certain attribute
	 *
	 * @param strList
	 *            attribute value in the form of ArrayList
	 * @param query
	 *            query string
	 * @return 1 means query exists, 0 otherwise
	 */
	public Double exists(ArrayList<String> strList, String query) {
		Double val = 0.0;
		if (strList != null) {
			String str = String.join(", ", strList);
			if (str != null && str.length() != 0) {
				if (str.toLowerCase().trim().contains(query)) {
					val = 1.0;
				}
			}
		}
		return val;
	}

	private double[] predictQueryTopic(String query) {
		double[] queryTopicProps = null;
		if (query.equals("")) {
			return queryTopicProps;
		}

		queryTopicProps = lda.predict(query);
		return queryTopicProps;
	}

	/**
	 * Main method of semantic search
	 *
	 * @param index
	 *            index name in Elasticsearch
	 * @param type
	 *            type name in Elasticsearch
	 * @param query
	 *            regular query string
	 * @param query_operator
	 *            query mode- query, or, and
	 * @return a list of search result
	 * @throws ParseException
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> searchByQuery(String index, String type, String query, String query_operator)
			throws ParseException {
		boolean exists = es.getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
		if (!exists) {
			return null;
		}

		double[] queryTopics = this.predictQueryTopic(query);

		Dispatcher dp = new Dispatcher(this.getConfig(), this.getES(), null);
		QueryBuilder qb = dp.createQuery(query, 1.0, query_operator);
		List<SResult> resultList = new ArrayList<SResult>();
		SearchRequestBuilder searchBuilder = es.getClient().prepareSearch(index).setTypes(type).setQuery(qb)
				.setSize(500)/* .addHighlightedField("title") */.addHighlightedField("content")
				.setHighlighterPreTags("<b>").setHighlighterPostTags("</b>").setHighlighterFragmentSize(500)
				.setHighlighterNumOfFragments(1);
		int docCount = this.getES().getDocCount(index, qb, type);

		SearchResponse response = searchBuilder.execute().actionGet();
		for (SearchHit hit : response.getHits().getHits()) {

			Map<String, Object> result = hit.getSource();
			String fileType = (String) result.get("fileType");
			String Time = (String) result.get("tstamp");
			String Content = (String) result.get("content");
			// String chunkerKeyword = (String) result.get("chunker_keywords");
			// String keywords = (String) result.get("original_keywords");
			String goldKeywords = (String) result.get("gold_keywords");
			String date = (String) result.get("date");
			long datelong = (long) result.get("datelong");

			String summary = Content;
			String Title, URL = null;
			Title = (String) result.get("title");
			URL = (String) result.get("url");

			// get highlight content
			Map<String, HighlightField> highlights = hit.highlightFields();
			for (String s : highlights.keySet()) {
				HighlightField highlight = highlights.get(s);
				Text[] texts = highlight.getFragments();
				/*
				 * if (s.equals("title")) { Title = texts[0].toString(); }
				 */

				if (s.equals("content")) {
					summary = texts[0].toString();
				}
			}

			// score related field
			// term relevance
			Double relevance = Double.valueOf(NDForm.format(hit.getScore()));
			// topic relevance
			double topicRelevance = 0.0;
			List<Double> doctopic = (List<Double>) result.get("topicProps");
			if (queryTopics != null && doctopic != null) {
				Double[] doctopicArray = doctopic.toArray(new Double[0]);
				for (int j = 0; j < queryTopics.length; j++) {
					topicRelevance += queryTopics[j] * doctopicArray[j];
				}
			}
			// web page rank
			double webpageRank = 1.0;
			if (result.containsKey("webpageRank")) {
				webpageRank = (double) result.get("webpageRank");
			}
			// host rank
			double hostRank = 1.0;
			if (result.containsKey("hostrank")) {
				hostRank = (double) result.get("hostrank");
			}

			String allKeywords = (String) result.get("gold_keywords");
			SResult re = new SResult(URL, Title, fileType, Content, summary, allKeywords, date);
			SResult.set(re, "term", relevance);
			SResult.set(re, "topic", topicRelevance);
			SResult.set(re, "pagerank", webpageRank);
			SResult.set(re, "hostrank", hostRank);
			SResult.set(re, "lastModifiedDate", Long.valueOf(datelong).doubleValue());
			resultList.add(re);
		}

		Map<String, Object> queryResult = new HashMap<String, Object>();
		queryResult.put("docCount", docCount);
		queryResult.put("docs", resultList);

		return queryResult;
	}

	/**
	 * Method of semantic search to generate JSON string
	 *
	 * @param index
	 *            index name in Elasticsearch
	 * @param type
	 *            type name in Elasticsearch
	 * @param query
	 *            regular query string
	 * @param query_operator
	 *            query mode- query, or, and
	 * @param rr
	 *            selected ranking method
	 * @return search results
	 * @throws ParseException
	 */
	public String ssearch(String index, String type, String query, String query_operator, Ranker rr)
			throws ParseException {
		Map<String, Object> searchResult = searchByQuery(index, type, query, query_operator);

		List<SResult> resultList = (List<SResult>) searchResult.get("docs");
		/*
		 * for(int i=0; i<resultList.size(); i++){
		 * System.out.println(resultList.get(i).printString()); }
		 */
		List<SResult> li = resultList;
		if (rr != null) {
			li = rr.rank(resultList);
		}
		Gson gson = new Gson();
		List<JsonObject> fileList = new ArrayList<>();
		if (li != null) {
			for (int i = 0; i < li.size(); i++) {
				JsonObject file = new JsonObject();
				file.addProperty("Title", (String) SResult.get(li.get(i), "title"));
				file.addProperty("Content", (String) SResult.get(li.get(i), "content"));
				file.addProperty("summary", (String) SResult.get(li.get(i), "summary"));
				file.addProperty("allKeywords", (String) SResult.get(li.get(i), "keywords"));
				file.addProperty("score", li.get(i).printString());
				file.addProperty("URL", (String) SResult.get(li.get(i), "url"));
				fileList.add(file);
			}
		}
		JsonElement fileListElement = gson.toJsonTree(fileList);
		JsonObject PDResults = new JsonObject();
		PDResults.add("SearchResults", fileListElement);
		PDResults.addProperty("ResultCount", (int) searchResult.get("docCount"));
		// System.out.println(PDResults.toString());
		return PDResults.toString();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CrawlerEngine me = new CrawlerEngine();
		me.loadConfig();
		SparkDriver spark = new SparkDriver(me.getConfig());
		ESDriver es = new ESDriver(me.getConfig());

		Searcher sr = new Searcher(me.loadConfig(), es, spark);
		Ranker rr = new Ranker(me.loadConfig(), es, spark);

		String fileList = null;
		try {
			fileList = sr.ssearch(me.getConfig().getProperty(CrawlerConstants.ES_INDEX_NAME),
					me.getConfig().getProperty(CrawlerConstants.CRAWLER_TYPE_NAME), "neo", "and", // please
					// replace
					// it
					// with
					// and,
					// or,
					// phrase
					rr);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// sr.searchQueryTopic("near earch topic");
		// sr.searchQueryTopic("neo");
		// System.out.print(fileList);
	}
}
