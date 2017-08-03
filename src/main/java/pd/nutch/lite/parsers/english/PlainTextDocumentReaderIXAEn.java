/*
 * Copyright (C) 2014 Angel Conde Manjon neuw84 at gmail.com
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package pd.nutch.lite.parsers.english;

import pd.nutch.lite.model.Token;
import pd.nutch.lite.parsers.AbstractDocumentReader;
import es.ehu.si.ixa.ixa.pipe.tok.Annotate;
import ixa.kaflib.Chunk;
import ixa.kaflib.KAFDocument;
import ixa.kaflib.Term;
import ixa.kaflib.WF;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author angel
 */
public class PlainTextDocumentReaderIXAEn extends AbstractDocumentReader {

  private transient final org.slf4j.Logger logger = LoggerFactory.getLogger(PlainTextDocumentReaderIXAEn.class);
  
  @Override
  public void readSource(String pSource) {
    KAFDocument kaf;
    try {
      BufferedReader breader = null;
      Properties properties = setAnnotateProperties("en", "IxaPipeTokenizer", "default", "false");
      InputStream instream = new ByteArrayInputStream(pSource.getBytes("UTF-8"));
      breader = new BufferedReader(new InputStreamReader(instream, "UTF-8"));
      kaf = new KAFDocument("en", "angel");
      KAFDocument.LinguisticProcessor newLp = kaf.addLinguisticProcessor("text", "ixa-pipe-tok-" + "en", "angel");
      newLp.setBeginTimestamp();
      Annotate TOKannotator = new Annotate(new BufferedReader(breader), properties);
      TOKannotator.tokenizedToKAF(kaf);
      newLp.setEndTimestamp();
      Properties posProperties = setAnnotatePropertiesPos("resources/lite/pos-models/en/en-pos-perceptron-c0-b3-dev.bin", "en", "3", "false", "false", "resources/lite/");
      pd.nutch.lite.utils.ixatools.pos.pos.Annotate POSannotator = new pd.nutch.lite.utils.ixatools.pos.pos.Annotate(posProperties);
      KAFDocument.LinguisticProcessor newLp2 = kaf.addLinguisticProcessor("terms", "ixa-pipe-pos-" + "en", "angel");
      newLp2.setBeginTimestamp();
      POSannotator.annotatePOSToKAF(kaf);
      newLp2.setEndTimestamp();
      pd.nutch.lite.utils.ixatools.chunk.Annotate chunkAnnotator = new pd.nutch.lite.utils.ixatools.chunk.Annotate("resources/lite/chunk-models/", "en");
      KAFDocument.LinguisticProcessor newLp3 = kaf.addLinguisticProcessor("chunks", "ixa-pipe-chunk-" + "en", "angel");
      newLp3.setBeginTimestamp();
      chunkAnnotator.chunkToKAF(kaf);
      newLp3.setEndTimestamp();
      for (int i = 0; i < kaf.getSentences().size(); i++) {
        List<WF> sentence = kaf.getSentences().get(i);
        StringBuilder sb = new StringBuilder();
        for (WF wfSent : sentence) {
          sb.append(wfSent.getForm()).append(" ");
        }

        String sentenceText = sb.toString().trim();
        if(!noiseSentences.contains(sentenceText.toLowerCase())){
          sentenceList.add(sentenceText);
        }
        
        LinkedList<Token> tokenList = new LinkedList<>();
        List<Term> terms = kaf.getSentenceTerms(i + 1);
        List<Chunk> chunksBySent = kaf.getChunksBySent(i + 1);
        boolean empty = false;
        int aux = 0;
        int numChunk = 0;
        int prev = 0;
        for (int j = 0; j < terms.size(); j++) {
          Term term = terms.get(j);
          if (j == aux) {
            if (!chunksBySent.isEmpty()) {
              if (numChunk < chunksBySent.size()) {
                Chunk chunk = chunksBySent.get(numChunk);
                if (term.getId() == null ? chunk.getTerms().get(0).getId() == null : term.getId().equals(chunk.getTerms().get(0).getId())) {
                  aux += chunk.getTerms().size();
                  empty = false;
                  Token tok = new Token(term.getForm(), term.getMorphofeat(), term.getLemma(), "B-" + chunk.getPhrase());
                  tokenList.add(tok);
                  prev = numChunk;
                  numChunk++;
                } else {
                  Token tok = new Token(term.getForm(), term.getMorphofeat(), term.getLemma(), "O");
                  tokenList.add(tok);
                }
              } else {
                Token tok = new Token(term.getForm(), term.getMorphofeat(), term.getLemma(), "O");
                tokenList.add(tok);
              }
            } else {
              empty = true;
            }
          } else {
            if (empty) {
              Token tok = new Token(term.getForm(), term.getMorphofeat(), term.getLemma(), "O");
              tokenList.add(tok);
            } else {
              Chunk chunk = chunksBySent.get(prev);
              Token tok = new Token(term.getForm(), term.getMorphofeat(), term.getLemma(), "I-" + chunk.getPhrase());
              tokenList.add(tok);
            }
          }
        }
        tokenizedSentenceList.add(tokenList);
      }
    } catch (FileNotFoundException ex) {
      logger.error("File not found ", ex);
    } catch (UnsupportedEncodingException ex) {
      logger.error("Encoding error ", ex);
    } catch (IOException ex) {
      logger.error("IO Exceptio n", ex);
    }

  }

  private Properties setAnnotateProperties(String lang, String tokenizer, String normalize, String paragraphs) {
    Properties annotateProperties = new Properties();
    annotateProperties.setProperty("language", lang);
    annotateProperties.setProperty("tokenizer", tokenizer);
    annotateProperties.setProperty("normalize", normalize);
    annotateProperties.setProperty("paragraphs", paragraphs);
    return annotateProperties;
  }

  private Properties setAnnotatePropertiesPos(final String model, final String language, final String beamSize, final String multiwords, final String dictag, String directory) {
    final Properties annotateProperties = new Properties();
    annotateProperties.setProperty("model", model);
    annotateProperties.setProperty("language", language);
    annotateProperties.setProperty("beamSize", beamSize);
    annotateProperties.setProperty("multiwords", multiwords);
    annotateProperties.setProperty("dictag", dictag);
    annotateProperties.setProperty("directory", directory);
    return annotateProperties;
  }

@Override
public void readSentences(String pSource) {
	// TODO Auto-generated method stub
	KAFDocument kaf;
    try {
      BufferedReader breader = null;
      Properties properties = setAnnotateProperties("en", "IxaPipeTokenizer", "default", "false");
      InputStream instream = new ByteArrayInputStream(pSource.getBytes("UTF-8"));
      breader = new BufferedReader(new InputStreamReader(instream, "UTF-8"));
      kaf = new KAFDocument("en", "angel");
      KAFDocument.LinguisticProcessor newLp = kaf.addLinguisticProcessor("text", "ixa-pipe-tok-" + "en", "angel");
      newLp.setBeginTimestamp();
      Annotate TOKannotator = new Annotate(new BufferedReader(breader), properties);
      TOKannotator.tokenizedToKAF(kaf);
      newLp.setEndTimestamp();
      for (int i = 0; i < kaf.getSentences().size(); i++) {
        List<WF> sentence = kaf.getSentences().get(i);
        StringBuilder sb = new StringBuilder();
        for (WF wfSent : sentence) {
          sb.append(wfSent.getForm()).append(" ");
        }
        
        String sentenceText = sb.toString().trim();
        if(!noiseSentences.contains(sentenceText)){
          sentenceList.add(sentenceText);
        }
      }
    } catch (UnsupportedEncodingException ex) {
      logger.error("Encoding error ", ex);
    } catch (IOException ex) {
      logger.error("IO Exceptio n", ex);
    }

}
}
