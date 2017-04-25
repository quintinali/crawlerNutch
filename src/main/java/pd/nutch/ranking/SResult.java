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

import java.lang.reflect.Field;

/**
 * Data structure class for search result
 */
public class SResult {
	public static final String rlist[] = { "term_score", "topic_score", "pagerank_score","hostrank_score"};
	
	public static final double rweights[] = { 0.5, 0.3, 0.1, 0.1};

	String title = null;
	String fileType = null;
	String content = null;
	String summary = null;
	String keywords = null;
	String url = null;
	
	public Double term = null;
	public Double topic = null;
	public Double pagerank = null;
	public Double hostrank = null;

	public Double final_score = 0.0;
	public Double term_score = 0.0;
	public Double topic_score = 0.0;
	public Double pagerank_score = 1.0;
	public Double hostrank_score = 1.0;
	  
	/**
	 * @param shortName
	 *            short name of dataset
	 * @param longName
	 *            long name of dataset
	 * @param topic
	 *            topic of dataset
	 * @param description
	 *            description of dataset
	 * @param date
	 *            release date of dataset
	 */
	public SResult(String url, String title, String fileType, String content, String summary, String keywords) {
		this.url = url;
		this.title = title;
		this.fileType = fileType;
		this.content = content;
		this.summary = summary;
		this.keywords = keywords;
	}

	public SResult(SResult sr) {
		for (int i = 0; i < rlist.length; i++) {
			set(this, rlist[i], get(sr, rlist[i]));
		}
	}

	/**
	 * Generic setter method
	 * 
	 * @param object
	 *            instance of SResult
	 * @param fieldName
	 *            field name that needs to be set on
	 * @param fieldValue
	 *            field value that needs to be set to
	 * @return 1 means success, and 0 otherwise
	 */
	public static boolean set(Object object, String fieldName, Object fieldValue) {
		Class<?> clazz = object.getClass();
		while (clazz != null) {
			try {
				Field field = clazz.getDeclaredField(fieldName);
				field.setAccessible(true);
				field.set(object, fieldValue);
				return true;
			} catch (NoSuchFieldException e) {
				clazz = clazz.getSuperclass();
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		return false;
	}
	
	public static boolean setFinalScore(Object object) {
		Class<?> clazz = object.getClass();
		while (clazz != null) {
			try {
				double finalscore = 0.0;
				 for(int m =0; m <SResult.rlist.length; m++)
			      {
			        String att = SResult.rlist[m].split("_")[0];
			        double weight = SResult.rweights[m];
			        double att_score = (Double)SResult.get(object, att);
			        finalscore += att_score * weight;
			      }
				 
				 SResult.set(object, "final_score", finalscore);
				
				return true;
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		return false;
	}

	/**
	 * Generic getter method
	 * 
	 * @param object
	 *            instance of SResult
	 * @param fieldName
	 *            field name of search result
	 * @param <V>
	 *            data type
	 * @return the value of the filed in the object
	 */
	@SuppressWarnings("unchecked")
	public static <V> V get(Object object, String fieldName) {
		Class<?> clazz = object.getClass();
		while (clazz != null) {
			try {
				Field field = clazz.getDeclaredField(fieldName);
				field.setAccessible(true);
				return (V) field.get(object);
			} catch (NoSuchFieldException e) {
				clazz = clazz.getSuperclass();
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		return null;
	}

	public String printString() {
		return term + ",  " + topic + ",  " + pagerank + ",  " + hostrank + ",  " + final_score;
	}

}
