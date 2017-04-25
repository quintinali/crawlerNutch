package pd.nutch.ranking.webpage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import scala.collection.JavaConversions;
import scala.collection.Seq;
import edu.knowitall.openie.Argument;
import edu.knowitall.openie.Instance;
import edu.knowitall.openie.OpenIE;
import edu.knowitall.tool.parse.ClearParser;
import edu.knowitall.tool.postag.ClearPostagger;
import edu.knowitall.tool.srl.ClearSrl;
import edu.knowitall.tool.tokenize.ClearTokenizer;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerAbstract;
import pd.nutch.main.CrawlerConstants;
import pd.nutch.main.CrawlerEngine;

/**
 * Read a list text files from a folder, and write all the results to another folder
 * @author Yongyao
 *
 */
public class STCopenIE {
	
	static String readFile(String path, Charset encoding) 
			throws IOException 
	{
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	public void openIE() throws IOException {
		// TODO Auto-generated method stub
		String path = "D:/crawlerData/";
		File output_file = new File(path + "dataoutput/" + UUID.randomUUID().toString());
		System.out.println(output_file.toString());
		output_file.createNewFile();
		FileWriter fw = new FileWriter(output_file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
			
		SentenceDetector sentenceDetector = null;
		InputStream modelIn = null;

		try {
			
			File fi = new File(path + "en-sent.bin");
			modelIn = new FileInputStream(fi);
			final SentenceModel sentenceModel = new SentenceModel(modelIn);
			modelIn.close();
			sentenceDetector = new SentenceDetectorME(sentenceModel);
		}
		catch (final IOException ioe) {
			ioe.printStackTrace();
		}
		System.out.println("---Started---");

		OpenIE openIE = new OpenIE(new ClearParser(new ClearPostagger(new ClearTokenizer())), new ClearSrl(), false, false);
		File directory = new File(path + "data/");
		File[] fList = directory.listFiles();
		for (File file : fList) {
			try {
				String text = readFile(file.getAbsolutePath(), StandardCharsets.UTF_8);
				String sentences[]= sentenceDetector.sentDetect(text);
				for(int i=0; i<sentences.length; i++)
				{
					if(sentences[i].contains("."))
					{
						Seq<Instance> extractions = openIE.extract(sentences[i]);

						List<Instance> list_extractions = JavaConversions.seqAsJavaList(extractions);
						for(Instance instance : list_extractions) {
							String delimiter = "&&";
							StringBuilder sb = new StringBuilder();

							sb.append(instance.confidence())
							.append(delimiter)
							.append(instance.extr().context())
							.append(delimiter)
							.append(instance.extr().arg1().text())
							.append(delimiter)
							.append(instance.extr().rel().text())
							.append(delimiter);

							List<Argument> list_arg2s = JavaConversions.seqAsJavaList(instance.extr().arg2s());
							for(Argument argument : list_arg2s) {
								sb.append(argument.text());
							}
							bw.write(sb.toString() + "\n");
							System.out.println(sb.toString());
						}
					}
				}


			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		bw.close();
		modelIn.close();


	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		STCopenIE openIE = new STCopenIE();
		try {
			openIE.openIE();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

