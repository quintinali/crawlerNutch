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
package pd.nutch.main;

import java.beans.Transient;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;
import javax.annotation.CheckForNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;

/**
 * This is the most generic class of Mudrod
 */
public abstract class CrawlerAbstract implements Serializable {

	protected transient ESDriver es = null;
	protected SparkDriver spark = null;
	protected Properties props = new Properties();

	protected static final String ES_SETTINGS = "elastic_settings.json";
	protected static final String ES_MAPPINGS = "elastic_mappings.json";

	//public transient FileWriter fw;
	//public transient BufferedWriter bw;
	/**
	 * Method of setting up essential configuration for MUDROD to start
	 */
	@CheckForNull
	protected void initCrawler() {
		//this.initCrawler(props.getProperty(CrawlerConstants.ES_INDEX_NAME));
		this.initCrawler("parse");
	}

	protected void initCrawler(String index) {
		InputStream settingsStream = getClass().getClassLoader().getResourceAsStream(ES_SETTINGS);
		InputStream mappingsStream = getClass().getClassLoader().getResourceAsStream(ES_MAPPINGS);
		JsonObject settingsJSON = null;
		JsonObject mappingJSON = null;
		
		JsonParser parser = new JsonParser();
		JsonObject o = parser.parse("{\"a\": \"A\"}").getAsJsonObject();

		try {
			settingsJSON = parser.parse(IOUtils.toString(settingsStream)).getAsJsonObject();
		} catch (IOException e1) {
			System.out.println("Error reading Elasticsearch settings!");
		}

		try {
			mappingJSON = parser.parse(IOUtils.toString(mappingsStream)).getAsJsonObject();
		} catch (IOException e1) {
			System.out.println("Error reading Elasticsearch mappings!");
		}

		try {
			if (settingsJSON != null && mappingJSON != null) {
				this.es.putMapping(index, settingsJSON.toString(), mappingJSON.toString());
			}
		} catch (IOException e) {
			System.out.println("Error entering Elasticsearch Mappings!");
		}
	}

	/**
	 * Get driver of Elasticsearch
	 * 
	 * @return driver of Elasticsearch
	 */
	public ESDriver getES() {
		return this.es;
	}

	public Properties getConfig() {
		return this.props;
	}

	public CrawlerAbstract(Properties props, ESDriver es, SparkDriver spark) {
		this.es = es;
		this.spark = spark;
		this.props = props;

		this.initCrawler();
	}
	
	public FileWriter createFile(String fileName) {
		FileWriter fw = null;
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
			//bw = new BufferedWriter(fw);
		} catch (IOException e) {
			e.printStackTrace();
		}
		 
		return fw;
	}}
