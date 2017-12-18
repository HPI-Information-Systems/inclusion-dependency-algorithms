package de.metanome.algorithms.zigzag;


import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.zigzag.configuration.ZigzagConfiguration;
import java.util.ArrayList;
import java.util.List;

public class Zigzag {

  private final ZigzagConfiguration configuration;
  private int currentLevel;
  private List<InclusionDependency> candidates;

  public Zigzag(ZigzagConfiguration configuration) {
    this.configuration = configuration;
    currentLevel = configuration.getK();
  }

  public void execute() throws AlgorithmExecutionException {
    // Calculate UINDs with other algorithm
    // Check candidates up until level k with other algorithm
    // - Add satisfied INDs and use unsatisfied INDs to prune candidates
    // Generate Hypergraph (candidates) from UINDs (do after check till level k, for better evaluation)
    // Calculate optimistic boarder from unsatisfied INDs
    // Jump to optimistic border
    // Check candidate using g3'
    // a. g3' == 0 => satisfied IND
    // b. g3' < epsilon => Travers adjacent nodes
    //          - check with g3' until all g'3 > epsilon
    // c. g3' > epsilon => Check k+1 level with other algorithm REPEAT
    // STOP when candidates = {}
    // Need to find a high arity dataset

    List<InclusionDependency> uINDs = calculateUnaryInclusionDependencies();
    initialCandidateCheck(configuration.getK());
    Hypergraph hypergraph = new Hypergraph(uINDs);
    hypergraph.addUnsatisfiedINDs();
    hypergraph.addSatisfiedINDs();


  }

  private void generateHypergraph(List<InclusionDependency> uINDs) {
    
  }

  private void initialCandidateCheck(int k) {
    for(int i = 0; i < k; i++) {
      checkCandidatesForLevel(i);
    }
  }

  private void checkCandidatesForLevel(int i) {

  }

  private List<InclusionDependency> calculateUnaryInclusionDependencies() {
    return new ArrayList<InclusionDependency>();
  }
}