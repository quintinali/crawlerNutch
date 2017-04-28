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

import java.util.Properties;

import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerAbstract;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Supports the ability to calculating ranking score
 */
public class Ranker extends CrawlerAbstract implements Serializable {

	public Ranker(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Method of calculating mean value
	 * 
	 * @param attribute
	 *            the attribute name that need to be calculated on
	 * @param resultList
	 *            an array list of result
	 * @return mean value
	 */
	private double getMean(String attribute, List<SResult> resultList) {
		double sum = 0.0;
		for (SResult a : resultList) {
			sum += (double) SResult.get(a, attribute);
		}
		return getNDForm(sum / resultList.size());
	}

	/**
	 * Method of calculating variance value
	 * 
	 * @param attribute
	 *            the attribute name that need to be calculated on
	 * @param resultList
	 *            an array list of result
	 * @return variance value
	 */
	private double getVariance(String attribute, List<SResult> resultList) {
		double mean = getMean(attribute, resultList);
		double temp = 0.0;
		double val = 0.0;
		for (SResult a : resultList) {
			val = (Double) SResult.get(a, attribute);
			temp += (mean - val) * (mean - val);
		}

		return getNDForm(temp / resultList.size());
	}

	/**
	 * Method of calculating standard variance
	 * 
	 * @param attribute
	 *            the attribute name that need to be calculated on
	 * @param resultList
	 *            an array list of result
	 * @return standard variance
	 */
	private double getStdDev(String attribute, List<SResult> resultList) {
		return getNDForm(Math.sqrt(getVariance(attribute, resultList)));
	}

	/**
	 * Method of calculating Z score
	 * 
	 * @param val
	 *            the value of an attribute
	 * @param mean
	 *            the mean value of an attribute
	 * @param std
	 *            the standard deviation of an attribute
	 * @return Z score
	 */
	private double getZscore(double val, double mean, double std) {
		if (std != 0) {
			return getNDForm((val - mean) / std);
		} else {
			return 0;
		}
	}

	/**
	 * Get the first N decimals of a double value
	 * 
	 * @param d
	 *            double value that needs to be processed
	 * @return processed double value
	 */
	private double getNDForm(double d) {
		DecimalFormat NDForm = new DecimalFormat("#.###");
		return Double.valueOf(NDForm.format(d));
	}
	
	private List<Double> getAttrValues(String attribute, List<SResult> resultList) {

		List<Double> values = new ArrayList<Double>();
		for (SResult a : resultList) {
			double value = (double) SResult.get(a, attribute);
			values.add(value);
		}	
		return values;
	}
	
	private double normalize(double val, double min, double max) {
		if(min != max){
			return getNDForm((val - min) / (max - min));
		}else{
			return 0.0;
		}
	}

	/**
	 * Method of ranking a list of result
	 * 
	 * @param resultList
	 *            result list
	 * @return ranked result list
	 */
	public List<SResult> rank(List<SResult> resultList) {
		for (int i = 0; i < resultList.size(); i++) {
			for (int m = 0; m < SResult.rlist.length; m++) {
				String att = SResult.rlist[m].split("_")[0];
				double val = SResult.get(resultList.get(i), att);
				/*double mean = getMean(att, resultList);
				double std = getStdDev(att, resultList);
				double score = getZscore(val, mean, std);*/
				
				List<Double> attValues = getAttrValues(att, resultList);
		        double min = Collections.min(attValues);
		        double max = Collections.max(attValues);
				double score = normalize(val, min, max);
				
				String score_id = SResult.rlist[m];
				SResult.set(resultList.get(i), score_id, score);
			}

			SResult.setFinalScore(resultList.get(i));
		}

		Collections.sort(resultList, new ResultComparator());
		return resultList;
	}

	public class ResultComparator implements Comparator<SResult> {
		@Override
		public int compare(SResult o1, SResult o2) {
			return o2.final_score.compareTo(o1.final_score);
		}
	}
}
