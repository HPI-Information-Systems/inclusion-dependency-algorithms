package de.metanome.algorithms.zigzag;


import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.zigzag.configuration.ZigzagConfiguration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javafx.util.Pair;

public class Zigzag {

  private final ZigzagConfiguration configuration;
  private int currentLevel;
  private Set<InclusionDependency> satisfiedINDs;
  private Set<InclusionDependency> unsatisfiedINDs;

  public Zigzag(ZigzagConfiguration configuration) {
    this.configuration = configuration;
    currentLevel = configuration.getK();
  }

  public void execute() throws AlgorithmExecutionException {
    Set<InclusionDependency> uINDs = calculateUnaryInclusionDependencies();
    initialCandidateCheck(configuration.getK());

    // TODO when borders are updated always reduce to the most specialized/generalized INDs
    Set<InclusionDependency> positiveBorder = satisfiedINDs; // Bd+(I)
    Set<InclusionDependency> negativeBorder = unsatisfiedINDs; // Bd-(I)

    Set<InclusionDependency> optimisticBorder = calculateOptimisticBorder(unsatisfiedINDs); // Bd+(I opt)

    while(!isOptimisticBorderFinal(optimisticBorder, positiveBorder)) {
      Set<InclusionDependency> possibleSmallerIND = new HashSet<>(); // optDI, all g3' < epsilon
      Set<InclusionDependency> pessDI = new HashSet<>(); // pessDI, all g3' > epsilon

      for(InclusionDependency ind : optimisticBorder) {
        int g3Val = g3(ind);
        if(g3Val == 0) {
          positiveBorder.add(ind);
          positiveBorder.remove(null);
        } else {
          negativeBorder.add(ind);
          if(g3Val <= configuration.getEpsilon()) {
            possibleSmallerIND.add(ind);
          } else {
            pessDI.add(ind);
          }
        }
      }
      while(!possibleSmallerIND.isEmpty()) {
        Set<InclusionDependency> candidates = possibleSmallerIND;
        for(InclusionDependency ind : candidates) {
          if(isIND(ind)) {
            positiveBorder.add(ind);
            candidates.remove(ind);
          } else {
            negativeBorder.add(ind);
          }
        }
        possibleSmallerIND = candidates;
      }
      // TODO check INDs which generalize pessDI
      Set<InclusionDependency> C = pessDI;
      for(InclusionDependency ind : C) {
        if(positiveBorder.contains(ind) || isIND(ind)) {
          positiveBorder.add(ind);
        } else {
          negativeBorder.add(ind);
        }
      }


      currentLevel += 1;
      optimisticBorder = calculateOptimisticBorder(unsatisfiedINDs);
    }

  }

  private boolean isOptimisticBorderFinal(Set<InclusionDependency> optimisticBorder, Set<InclusionDependency> positiveBorder) {
    optimisticBorder.removeAll(positiveBorder);
    return optimisticBorder.isEmpty();
  }

  // See Theorem 1
  private Set<InclusionDependency> calculateOptimisticBorder(Set<InclusionDependency> unsatisfiedINDs) {
    // TODO calculate optimistic border with theorem 1
    return new HashSet<>();
  }

  private int g3(InclusionDependency toCheck) {
    // TODO implement g3 on database
    return 1;
  }

  // equivalent to d |= i
  private boolean isIND(InclusionDependency ind) {
    return true; // TODO check if IND using other algorithm
  }

  private void initialCandidateCheck(int k) {
    Pair<InclusionDependency, Boolean> checkedIND;
    for(int i = 0; i < k; i++) {
      checkedIND = checkCandidatesForLevel(i);
      if(checkedIND.getValue()) {
        satisfiedINDs.add(checkedIND.getKey());
      } else {
        unsatisfiedINDs.add(checkedIND.getKey());
      }
    }
  }

  private Pair<InclusionDependency, Boolean> checkCandidatesForLevel(int i) {
    // TODO candidate check with other algorithm
    return new Pair<>(new InclusionDependency(null, null), true);
  }

  private Set<InclusionDependency> calculateUnaryInclusionDependencies() {
    // TODO calculate UINDs with other algorithm
    return new HashSet<>();
  }
}