package pd.nutch.lite.algorithms.ranked.unsupervised.cvalue.filters;

import pd.nutch.lite.model.Token;
import java.util.LinkedList;
import java.util.List;

/**
 * Each Lingustic filter must implement this interface This interface is
 * functional
 * 
 * @author Angel Conde Manjon
 */

@FunctionalInterface
public interface ILinguisticFilter {

  /**
   * each filter gets a sentence (Tokens) and returns a list of candidates (list
   * of strings)
   * 
   * @param pSentence
   * @return
   */
  public List<String> getCandidates(LinkedList<Token> pSentence);

}
