package pd.nutch.driver;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionFuzzyBuilder;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import pd.nutch.main.CrawlerConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class ESDriver {

	private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
	private static final long serialVersionUID = 1L;
	private transient Client client = null;
	private transient Node node = null;
	private transient BulkProcessor bulkProcessor = null;

	public ESDriver() {
		// Default constructor, to load configuration call
	}

	/**
	 * Substantiated constructor which accepts a {@link java.util.Properties}
	 * 
	 * @param props
	 *            a populated properties object.
	 */
	public ESDriver(Properties props) {
		try {
			setClient(makeClient(props));
		} catch (IOException e) {
			LOG.error("Error whilst constructing Elastcisearch client.", e);
		}
	}

	public void setClient(Client client) {
		this.client = client;
	}

	protected Client makeClient(Properties props) throws IOException {
		String clusterName = props.getProperty(CrawlerConstants.ES_CLUSTER);
		String hostsString = props.getProperty(CrawlerConstants.ES_UNICAST_HOSTS);
				
		String[] hosts = hostsString.split(",");
		String portStr = props.getProperty(CrawlerConstants.ES_TRANSPORT_TCP_PORT);
		int port = Integer.parseInt(portStr);

		Settings.Builder settingsBuilder = Settings.settingsBuilder();

		// Set the cluster name and build the settings
		if (StringUtils.isNotBlank(clusterName))
			settingsBuilder.put("cluster.name", clusterName);

		Settings settings = settingsBuilder.build();

		Client client = null;

		// Prefer TransportClient
		if (hosts != null && port > 1) {
			TransportClient transportClient = TransportClient.builder().settings(settings).build();
			for (String host : hosts)
				transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
			client = transportClient;
		} else if (clusterName != null) {
			node = nodeBuilder().settings(settings).client(true).node();
			client = node.client();
		}

		return client;
	}

	protected Client makeClient() throws IOException {
		String clusterName = "nutch";
		String hostsString = "127.0.0.1";
		String[] hosts = hostsString.split(",");
		String portStr = "9300";
		int port = Integer.parseInt(portStr);

		Settings.Builder settingsBuilder = Settings.settingsBuilder();

		// Set the cluster name and build the settings
		if (!clusterName.isEmpty())
			settingsBuilder.put("cluster.name", clusterName);

		Settings settings = settingsBuilder.build();

		Client client = null;

		// Prefer TransportClient
		if (hosts != null && port > 1) {
			TransportClient transportClient = TransportClient.builder().settings(settings).build();
			for (String host : hosts)
				transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
			client = transportClient;
		} else if (clusterName != null) {
			node = nodeBuilder().settings(settings).client(true).node();
			client = node.client();
		}

		/// System.out.println(node);
		// System.out.println(client);
		return client;
	}

	/**
	 * @return the client
	 */
	public Client getClient() {
		return client;
	}

	public void RefreshIndex() {
		node.client().admin().indices().prepareRefresh().execute().actionGet();
	}

	public void putMapping(String index) throws ElasticsearchException, IOException {
		boolean exists = client.admin().indices().prepareExists(index).execute().actionGet().isExists();
		if (exists) {
			return;
		}

		client.admin().indices().prepareCreate(index)
				.setSettings(Settings.settingsBuilder()
						.loadFromSource(jsonBuilder().startObject().startObject("analysis").startObject("filter")
								.startObject("cody_stop").field("type", "stop").field("stopwords", "_english_")
								.endObject().startObject("cody_stemmer").field("type", "stemmer")
								.field("stopwords", "light_english").endObject().endObject().startObject("analyzer")
								  .startObject("csv").field("type", "pattern")
								 .field("pattern", ",").endObject()
								 .startObject("cody").field("tokenizer", "standard")
								.field("filter", new String[] { "lowercase", "cody_stop" }).endObject().endObject()
								.endObject().endObject().string()))
				.execute().actionGet();

		XContentBuilder pageMapping = jsonBuilder().startObject().startObject("_default_").startObject("properties")
				.startObject("fileType").field("type", "string").field("index", "not_analyzed").endObject()
				.startObject("content_phrases_pos").field("type", "string").field("index", "not_analyzed").endObject()
				.startObject("title_phrases_pos").field("type", "string").field("index", "not_analyzed").endObject()
				.startObject("url").field("type", "string").field("index", "not_analyzed").endObject()
				.startObject("content_phrases").field("type", "string").field("index_analyzer", "csv").endObject()
				.startObject("title_phrases").field("type", "string").field("index_analyzer", "csv").endObject()
				.startObject("keywords").field("type", "string").field("index_analyzer", "csv").endObject().endObject()
				.endObject().endObject();

		client.admin().indices().preparePutMapping(index).setType("_default_").setSource(pageMapping).execute()
				.actionGet();

	}

	public void putMapping(String indexName, String settingsJson, String mappingJson) throws IOException {

		boolean exists = getClient().admin().indices().prepareExists(indexName).execute().actionGet().isExists();
		if (exists) {
			return;
		}

		getClient().admin().indices().prepareCreate(indexName)
				.setSettings(Settings.builder().loadFromSource(settingsJson)).execute().actionGet();
		getClient().admin().indices().preparePutMapping(indexName).setType("_default_").setSource(mappingJson).execute()
				.actionGet();
	}

	public void setBulkProcessor(BulkProcessor bulkProcessor) {
		this.bulkProcessor = bulkProcessor;
	}

	public BulkProcessor getBulkProcessor() {
		return bulkProcessor;
	}

	public void createBulkProcesser() {
		setBulkProcessor(BulkProcessor.builder(getClient(), new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				LOG.error("Bulk request has failed!");
				throw new RuntimeException("Caught exception in bulk: " + request + ", failure: " + failure, failure);
			}
		}).setBulkActions(1000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)).setConcurrentRequests(1).build());
	}

	public void destroyBulkProcessor() {
		try {
			getBulkProcessor().awaitClose(20, TimeUnit.MINUTES);
			setBulkProcessor(null);
			refreshIndex();
		} catch (InterruptedException e) {
			LOG.error("Error destroying the Bulk Processor.", e);
		}
	}

	public void refreshIndex() {
		client.admin().indices().prepareRefresh().execute().actionGet();
	}

	/*
	 * public boolean checkItemExist(String type, String keyName, String value)
	 * { CountResponse count = client.prepareCount(index).setTypes(type)
	 * .setQuery(QueryBuilders.termQuery(keyName, value)).execute().actionGet();
	 * 
	 * if (count.getCount() == 0) { return true; } else { return false; } }
	 */

	public boolean checkTypeExist(String index, String type) {
		GetMappingsResponse res;
		try {
			res = client.admin().indices().getMappings(new GetMappingsRequest().indices(index)).get();
			ImmutableOpenMap<String, MappingMetaData> mapping = res.mappings().get(index);
			for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
				if (c.key.equals(type)) {
					return true;
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public int getDocCount(String index, QueryBuilder filterSearch, String... type) {
		SearchRequestBuilder countSrBuilder = getClient().prepareSearch(index).setTypes(type).setQuery(filterSearch)
				.setSize(0);
		SearchResponse countSr = countSrBuilder.execute().actionGet();
		int docCount = (int) countSr.getHits().getTotalHits();
		return docCount;
	}

	public List<String> autoComplete(String index, String chars) {
		boolean exists = getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
		if (!exists) {
			return null;
		}

		List<String> SuggestList = new ArrayList<String>();

		CompletionSuggestionFuzzyBuilder suggestionsBuilder = new CompletionSuggestionFuzzyBuilder("completeMe");
		suggestionsBuilder.text(chars);
		suggestionsBuilder.size(10);
		suggestionsBuilder.field("name_suggest");
		suggestionsBuilder.setFuzziness(Fuzziness.fromEdits(2));

		SuggestRequestBuilder suggestRequestBuilder = client.prepareSuggest(index).addSuggestion(suggestionsBuilder);

		SuggestResponse suggestResponse = suggestRequestBuilder.execute().actionGet();

		Iterator<? extends Suggest.Suggestion.Entry.Option> iterator = suggestResponse.getSuggest()
				.getSuggestion("completeMe").iterator().next().getOptions().iterator();

		while (iterator.hasNext()) {
			Suggest.Suggestion.Entry.Option next = iterator.next();
			SuggestList.add(next.getText().string());
		}
		return SuggestList;

	}

	public String customAnalyzing(String indexName, String str) throws InterruptedException, ExecutionException {
		return this.customAnalyzing(indexName, "cody", str);
	}

	public String customAnalyzing(String indexName, String analyzer, String str)
			throws InterruptedException, ExecutionException {
		if(str == null){
			return "";
		}
		String[] strList = str.toLowerCase().split(",");
		for (int i = 0; i < strList.length; i++) {
			String tmp = "";
			AnalyzeResponse r = client.admin().indices().prepareAnalyze(strList[i]).setIndex(indexName)
					.setAnalyzer(analyzer).execute().get();
			for (AnalyzeToken token : r.getTokens()) {
				tmp += token.getTerm() + " ";
			}
			strList[i] = tmp.trim();
		}
		return String.join(",", strList);
	}

	public void deleteAllByQuery(String index, String type, QueryBuilder query) {
	    createBulkProcesser();
	    SearchResponse scrollResp = getClient().prepareSearch(index)
	        .setSearchType(SearchType.SCAN).setTypes(type)
	        .setScroll(new TimeValue(60000)).setQuery(query).setSize(10000)
	        .execute().actionGet();

	    while (true) {
	      for (SearchHit hit : scrollResp.getHits().getHits()) {
	        DeleteRequest deleteRequest = new DeleteRequest(index, type,
	            hit.getId());
	        getBulkProcessor().add(deleteRequest);
	      }

	      scrollResp = getClient().prepareSearchScroll(scrollResp.getScrollId())
	          .setScroll(new TimeValue(600000)).execute().actionGet();
	      if (scrollResp.getHits().getHits().length == 0) {
	        break;
	      }

	    }
	    destroyBulkProcessor();
	  }

	  public void deleteType(String index, String type) {
	    this.deleteAllByQuery(index, type, QueryBuilders.matchAllQuery());
	  }

	  
	public void closeES() throws InterruptedException {
		// bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		node.close();
	}

	public void close() {
		client.close();
	}
}
