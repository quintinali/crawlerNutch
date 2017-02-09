package pd.nutch.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import opennlp.tools.chunker.ChunkerME;
import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import pd.nutch.driver.ESdriver;
import pd.nutch.textrank.TextRankKeyword;

/**
 *
 * Extracts noun phrases from a sentence. To create sentences using OpenNLP use
 * the SentenceDetector classes.
 */
public class OpenNLPPhraseExtractor {

  String modelPath = "";
  SentenceDetectorME sentDetecter = null;
  TokenizerME wordBreaker = null;
  POSTaggerME posme = null;
  ChunkerME chunkerME = null;
  NameFinderME nameFinder = null;

  TextRankKeyword textRank = null;

  private static Pattern untokenizedParenPattern1 = Pattern
      .compile("([^ ])([({)}])");
  private static Pattern untokenizedParenPattern2 = Pattern
      .compile("([({)}])([^ ])");

  public OpenNLPPhraseExtractor() {

    String modelPath = "E:\\";

    try {
      SentenceModel sm = new SentenceModel(
          new FileInputStream(new File(modelPath + "en-sent.bin")));
      sentDetecter = new SentenceDetectorME(sm);

      TokenizerModel tm = new TokenizerModel(
          new FileInputStream(new File(modelPath + "en-token.bin")));
      wordBreaker = new TokenizerME(tm);

      POSModel pm = new POSModel(
          new FileInputStream(new File(modelPath + "en-pos-maxent.bin")));
      posme = new POSTaggerME(pm);

      InputStream modelIn = new FileInputStream(modelPath + "en-chunker.bin");
      ChunkerModel chunkerModel = new ChunkerModel(modelIn);
      chunkerME = new ChunkerME(chunkerModel);

      InputStream nameFinderIn = new FileInputStream(
          modelPath + "es-ner-organization.bin");
      TokenNameFinderModel nameFinderModel = new TokenNameFinderModel(
          nameFinderIn);
      NameFinderME nameFinder = new NameFinderME(nameFinderModel);

      textRank = new TextRankKeyword();
      textRank.nKeyword = 5;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public JsonObject NounPhraseExtractor(ESdriver es, String indexName,
      String text) throws InterruptedException, ExecutionException {

    JsonObject phraseJson = new JsonObject();
    if (text == null || text.equals("")) {

      phraseJson.addProperty("phrase", "");
      phraseJson.addProperty("phrasePos", "");

      return phraseJson;
    }

    List<String> phrases = new ArrayList<String>();
    Map<String, Integer> phrasePoss = new LinkedHashMap<String, Integer>();
    String[] sentences = sentDetecter.sentDetect(text);
    int sentNum = sentences.length;
    for (int i = 0; i < sentNum; i++) {
      String sentence = sentences[i];
      // tokenize brackets and parentheses by putting a space on either side.
      // this makes sure it doesn't get confused with output from the parser
      sentence = untokenizedParenPattern1.matcher(sentence).replaceAll("$1 $2");
      sentence = untokenizedParenPattern2.matcher(sentence).replaceAll("$1 $2");

      // is
      String[] words = wordBreaker.tokenize(sentence);

      // posTags are the parts of speech of every word in the sentence
      String[] posTags = posme.tag(words);

      // chunks
      Span[] chunks = chunkerME.chunkAsSpans(words, posTags);
      // chunkStrings are the actual chunks
      String[] chunkStrings = Span.spansToStrings(chunks, words);

      for (int j = 0; j < chunks.length; j++) {
        String np = chunkStrings[j];
        if (chunks[j].getType().equals("NP")) {

          String customNP = es.customAnalyzing(indexName, np);
          if (!customNP.isEmpty()) {
            phrases.add(customNP);
            int pos = chunks[j].getStart();
            phrasePoss.put(customNP, pos);
          }
        }
      }
    }

    String phrasePosJson = new Gson().toJson(phrasePoss);
    phraseJson.addProperty("phrase", String.join(",", phrases));
    phraseJson.addProperty("phrasePos", phrasePosJson);

    return phraseJson;
  }

  public String NERExtractor(String text) throws IOException {

    List<String> ners = new ArrayList<String>();
    String[] sentences = sentDetecter.sentDetect(text);
    Span nameSpans[] = nameFinder.find(sentences);
    for (Span s : nameSpans) {
      ners.add(s.toString());
    }

    return String.join(",", ners);
  }

  public String keyPhraseExtractor(JsonObject titleTerms,
      JsonObject contentTerms) throws IOException {

    String title = titleTerms.get("phrase").toString().replaceAll(" ", "-")
        .replaceAll(",", " ");
    String content = contentTerms.get("phrase").toString().replaceAll(" ", "-")
        .replaceAll(",", " ");

    String keywords = this.keyPhraseExtractor(title, content);
    keywords = keywords.replaceAll("-", " ");

    return keywords;
  }

  public String keyPhraseExtractor(String title, String content)
      throws IOException {

    if (title == null) {
      title = "";
    }

    if (content == null) {
      content = "";
    }

    String keywords = textRank.getKeyword(title, content);
    keywords = keywords.replaceAll("#", ",");

    return keywords;
  }

  public String gensimKeyPhraseExtractor(JsonObject titleTerms,
      JsonObject contentTerms) throws IOException {

    String title = titleTerms.get("phrase").toString().replaceAll(" ", "-")
        .replaceAll(",", " ");
    String content = contentTerms.get("phrase").toString().replaceAll(" ", "-")
        .replaceAll(",", " ");

    String keywords = this.gensimKeyPhraseExtractor(title, content);
    keywords = keywords.replaceAll("-", " ");

    return keywords;
  }

  public String gensimKeyPhraseExtractor(String title, String content)
      throws IOException {

    String keywords = "";
    String[] cmd = { "python", "D:/pythone workspace/test.py",
        title + ". " + content, };
    try {
      Process proc = Runtime.getRuntime().exec(cmd);

      BufferedReader stdInput = new BufferedReader(
          new InputStreamReader(proc.getInputStream()));

      BufferedReader stdError = new BufferedReader(
          new InputStreamReader(proc.getErrorStream()));

      // read the output from the command
      // System.out.println("Here is the standard output of the command:\n");
      String s = null;
      while ((s = stdInput.readLine()) != null) {
        keywords += s;
      }

      // read any errors from the attempted command
      /*System.out
          .println("Here is the standard error of the command (if any):\n");
      while ((s = stdError.readLine()) != null) {
        System.out.println(s);
      }*/
    } catch (IOException e3) {
      // TODO Auto-generated catch block
      e3.printStackTrace();
    }

    return keywords;
  }

  public static <PythonInterpreter> void main(String[] args) {

    String text = "Catalog Page for PIA19439 PIA19439: Five New Crater Names for Mercury  Target Name:, Mercury  Is a satellite of: Sol (our sun)  Mission: MESSENGER    Spacecraft: MESSENGER  Product Size: 2683 x 2175 pixels (w x h)   Produced By: Johns Hopkins University/APL    Full-Res TIFF: PIA19439.tif (17.51 MB)  Full-Res JPEG: PIA19439.jpg (599.6 kB)   Click on the image above to download a moderately sized image in JPEG format (possibly reduced in size from original) Original Caption Released with Image: Five previously unnamed craters on Mercury now have names. MESSENGER's Education and Public Outreach (EPO) team led a contest that solicited naming suggestions from the public via a competition website . In total, 3,600 contest entries were received and a semi-final list of 17 names were submitted to the International Astronomical Union (IAU) for consideration. The IAU selected the final five crater names, keeping with the convention that Mercury's craters are named after those who have made significant contributions to the humanities. And the winners are: Carolan : (83.8° N, 31.7° E) Named for Turlough O'Carolan, the Irish musician and composer (1670-1738) Enheduanna : (48.3° N, 326.2° E) Named for the author and poet from ancient Mesopotamia Karsh (35.6° S, 78.9° E) Named for Yousuf Karsh, twentieth century Armenian-Canadian portrait photographer Kulthum (50.7° N, 93.5° E) Named for Umm Kulthum, twentieth century Egyptian singer, songwriter, and actress Rivera : (69.3° N, 32.4° E) Named for Diego Rivera, twentieth century Mexican painter and muralist Watch an animation showing the locations of the five newly named craters, or view them on this graphic along with the IAU news release. Read the MESSENGER mission news story for additional details. The MESSENGER spacecraft is the first ever to orbit the planet Mercury, and the spacecraft's seven scientific instruments and radio science investigation are unraveling the history and evolution of the Solar System's innermost planet. In the mission's more than four years of orbital operations, MESSENGER has acquired over 250,000 images and extensive other data sets. MESSENGER's highly successful orbital mission is about to come to an end , as the spacecraft runs out of propellant and the force of solar gravity causes it to impact the surface of Mercury on April 30, 2015. For information regarding the use of images, see the MESSENGER image use policy . Image Credit: NASA/Johns Hopkins University Applied Physics Laboratory/Carnegie Institution of Washington Image Addition Date: 2015-04-29";

    ESdriver es = new ESdriver();
    try {
      es.putMapping("testindex");
    } catch (Exception e2) {
      // TODO Auto-generated catch block
      e2.printStackTrace();
    }

    OpenNLPPhraseExtractor extrctor = new OpenNLPPhraseExtractor();
    // String s = "Autobiography - Wikipedia";
    JsonObject terms = null;
    try {
      terms = extrctor.NounPhraseExtractor(es, "testindex", text);
    } catch (Exception e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    System.out.println(terms.get("phrase"));
    // System.out.println(terms.get("phrasePos"));

    String term = terms.get("phrase").toString().replaceAll(" ", "-")
        .replaceAll(",", " ");

    try {
      // String news = es.customAnalyzing("testindex", terms);
      // System.out.println(news);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
}