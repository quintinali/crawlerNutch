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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerAbstract;

/**
 * Supports ability to transform regular user query into a semantic query
 */
public class Dispatcher extends CrawlerAbstract {
	private static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);

	public Dispatcher(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
	}

	/**
	 * Method of getting semantically most related terms by number
	 * 
	 * @param input
	 *            regular input query
	 * @param num
	 *            the number of most related terms
	 * @return a map from term to similarity
	 */
	public Map<String, Double> getRelatedTerms(String input, int num) {

		Map<String, Double> selected_Map = new HashMap<>();

		return selected_Map;
	}

	/**
	 * Method of getting semantically most related terms by similarity threshold
	 * 
	 * @param input
	 *            regular input query
	 * @param T
	 *            value of threshold, raning from 0 to 1
	 * @return a map from term to similarity
	 */
	public Map<String, Double> getRelatedTermsByT(String input, double T) {
		Map<String, Double> selected_Map = new HashMap<>();
		// selected_Map.put(input, 1.0);
		return selected_Map;
	}

	/**
	 * Method of creating semantic query based on Threshold
	 * 
	 * @param input
	 *            regular query
	 * @param T
	 *            threshold raning from 0 to 1
	 * @param query_operator
	 *            query mode
	 * @return a multiMatch query builder
	 */
	public QueryBuilder createQuery(String input, double T, String query_operator) {

		if (input.equals("")) {
			QueryBuilder qb = null;
			qb = QueryBuilders.matchAllQuery();
			return qb;
		}

		Map<String, Double> selected_Map = getRelatedTermsByT(input, T);
		selected_Map.put(input, (double) 1);

		String fieldsList[] = { "title", "content", "anchor_inlinks", "url_inlinks", "url", "gold_keywords" };
		BoolQueryBuilder qb = new BoolQueryBuilder();
		for (Entry<String, Double> entry : selected_Map.entrySet()) {
			if (query_operator.equals("phrase")) {
				qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), fieldsList).boost(entry.getValue().floatValue())
						.type(MultiMatchQueryBuilder.Type.PHRASE).tieBreaker((float) 0.5)); // when
																							// set
																							// to
																							// 1.0,
																							// it
																							// would
																							// be
																							// equal
																							// to
																							// "most
																							// fields"
																							// query
			} else if (query_operator.equals("and")) {
				qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), fieldsList).boost(entry.getValue().floatValue())
						.operator(MatchQueryBuilder.Operator.AND).tieBreaker((float) 0.5));
			} else {
				qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), fieldsList).boost(entry.getValue().floatValue())
						.operator(MatchQueryBuilder.Operator.OR).tieBreaker((float) 0.5));
			}
		}

		// LOG.info(qb.toString());
		return qb;
	}
}
