package pd.nutch.partition;

import java.util.Map;

public interface ThePartitionProblemSolver {

  public Map<String, Integer> solve(Map<String, Double> labelNums, int k);
}
