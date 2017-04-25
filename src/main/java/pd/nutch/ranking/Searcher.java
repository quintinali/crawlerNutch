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

import java.io.Serializable;
import java.text.DecimalFormat;
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

/**
 * Supports ability to performance semantic search with a given query
 */
public class Searcher extends CrawlerAbstract implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
	DecimalFormat NDForm = new DecimalFormat("#.##");
	final Integer MAX_CHAR = 700;

	public Searcher(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
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
	
	private List<Double[]> searchQueryTopic(String query){
		String indexName = props.getProperty(CrawlerConstants.ES_INDEX_NAME);
		String wordtopicType = props.getProperty(CrawlerConstants.WORD_TOPIC_TYPE);
		try {
			query = es.customAnalyzing("parse", query);
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] queryWord = query.trim().split(" ");
		BoolQueryBuilder qb = new BoolQueryBuilder();
	    for (String word : queryWord) {
	        qb.should(QueryBuilders.multiMatchQuery(word, "word")
	            .operator(MatchQueryBuilder.Operator.OR).tieBreaker((float) 0.5));
	    }
	    
	    List<Double[]> queryTopicProps = new ArrayList<Double[]>();
		SearchRequestBuilder searchBuilder = es.getClient().prepareSearch(indexName).setTypes(wordtopicType).setQuery(qb)
				.setSize(500);
		SearchResponse response = searchBuilder.execute().actionGet();
		for (SearchHit hit : response.getHits().getHits()) {

			Map<String, Object> result = hit.getSource();
			String fileType = (String) result.get("fileType");
			List<Double> topicProps = (List<Double>) result.get("props");
			if(topicProps != null){
			Double[] topicPropsArray = topicProps.toArray(new Double[0]);
			queryTopicProps.add(topicPropsArray);
			}
		}
	      
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
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> searchByQuery(String index, String type, String query, String query_operator) {
		boolean exists = es.getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
		if (!exists) {
			return null;
		}
		
		List<Double[]> queryTopics = this.searchQueryTopic(query);

		Dispatcher dp = new Dispatcher(this.getConfig(), this.getES(), null);
		BoolQueryBuilder qb = dp.createQuery(query, 1.0, query_operator);
		List<SResult> resultList = new ArrayList<SResult>();
		SearchRequestBuilder searchBuilder = es.getClient().prepareSearch(index).setTypes(type).setQuery(qb)
				.setSize(500)/*.addHighlightedField("title")*/.addHighlightedField("content")
				.setHighlighterPreTags("<b>").setHighlighterPostTags("</b>").setHighlighterFragmentSize(500)
				.setHighlighterNumOfFragments(1);
		int docCount = this.getES().getDocCount(index, qb, type);

		SearchResponse response = searchBuilder.execute().actionGet();
		for (SearchHit hit : response.getHits().getHits()) {

			Map<String, Object> result = hit.getSource();
			String fileType = (String) result.get("fileType");
			String Time = (String) result.get("tstamp");
			String Content = (String) result.get("content");
			String chunkerKeyword = (String) result.get("chunker_keywords");
			String keywords = (String) result.get("original_keywords");
			String goldKeywords = (String) result.get("gold_keywords");
			String orgnization = (String) result.get("organization");
			String summary = Content;
			String Title, URL = null;
			Title = (String) result.get("title");
			URL = (String) result.get("url");

			// get highlight content
			Map<String, HighlightField> highlights = hit.highlightFields();
			for (String s : highlights.keySet()) {
				HighlightField highlight = highlights.get(s);
				Text[] texts = highlight.getFragments();
				/*if (s.equals("title")) {
					Title = texts[0].toString();
				}*/

				if (s.equals("content")) {
					summary = texts[0].toString();
				}
			}
			
			//score related field
			//term relevance 
			Double relevance = Double.valueOf(NDForm.format(hit.getScore()));
			//topic relevance
			double topicRelevance = 0.0;
			List<Double> doctopic = (List<Double>) result.get("topicProps");
			if(queryTopics.size() != 0 && doctopic != null){
				Double[] doctopicArray = doctopic.toArray(new Double[0]);
				for(int i=0; i<queryTopics.size(); i++){
					Double[] wordTopic = queryTopics.get(i);
					double worddocTopicRel = 0;
					for(int j=0; j<wordTopic.length; j++){
						worddocTopicRel += wordTopic[j]*doctopicArray[j];
					}
					topicRelevance += worddocTopicRel;
				}
			}
			//web page rank
			double webpageRank = 1.0;
			if(result.containsKey("webpageRank")){
				webpageRank = (double) result.get("webpageRank");
			}
			//host rank
			double hostRank = 1.0;
			if(result.containsKey("hostrank")){
				hostRank = (double) result.get("hostrank");
			}

			// combine keywords
			/*List<String> keywordList = new ArrayList<String>();
			if (keywords != null && keywords.length() > 0) {
				String[] keywordArray = keywords.split(",");
				for (String s : keywordArray) {
					keywordList.add(s);
				}
			}
			if (chunkerKeyword != null && chunkerKeyword.length() > 0) {
				String[] chunkerKeywordsArray = chunkerKeyword.split(",");
				for (String s : chunkerKeywordsArray) {
					if (s.split(" ").length < 5)
						keywordList.add(s);
				}
			}
			if (goldKeywords != null && goldKeywords.length() > 0) {
				String[] goldKeywordArray = goldKeywords.split(",");
				for (String s : goldKeywordArray) {
					keywordList.add(s);
				}
			}
			String allKeywords = "";
			Set<String> mySet = new HashSet<String>(keywordList);
			for (String s : mySet) {
				allKeywords += s + ", ";
			}
			if (allKeywords.length() > 1) {
				allKeywords = allKeywords.substring(0, allKeywords.length() - 1);
			}
*/
			String allKeywords = relevance + " | " + topicRelevance + " | " + webpageRank + " | " + hostRank;
			SResult re = new SResult(URL, Title, fileType, Content, summary, allKeywords);
			SResult.set(re, "term", relevance);
			SResult.set(re, "topic", topicRelevance);
			SResult.set(re, "pagerank", webpageRank);
			SResult.set(re, "hostrank", hostRank);
			//System.out.println(re.printString());
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
	 */
	public String ssearch(String index, String type, String query, String query_operator, Ranker rr) {
		Map<String, Object> searchResult = searchByQuery(index, type, query, query_operator);

		List<SResult> resultList = (List<SResult>) searchResult.get("docs");
		/*for(int i=0; i<resultList.size(); i++){
			System.out.println(resultList.get(i).printString());
		}*/
		List<SResult> li =resultList;
		if(rr != null){
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
				//file.addProperty("allKeywords", (String) SResult.get(li.get(i), "keywords"));
				file.addProperty("allKeywords", li.get(i).printString());
				file.addProperty("URL", (String) SResult.get(li.get(i), "url"));
				fileList.add(file);
			}
		}
		JsonElement fileListElement = gson.toJsonTree(fileList);
		JsonObject PDResults = new JsonObject();
		PDResults.add("SearchResults", fileListElement);
		PDResults.addProperty("ResultCount", (int) searchResult.get("docCount"));
		//System.out.println(PDResults.toString());
		return PDResults.toString();
	}
}
