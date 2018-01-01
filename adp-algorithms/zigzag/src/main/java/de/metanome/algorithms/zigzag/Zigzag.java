package de.metanome.algorithms.zigzag;


import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.zigzag.configuration.ZigzagConfiguration;
import de.metanome.util.InclusionDependencyBuilder;
import it.unimi.dsi.fastutil.Hash;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javafx.util.Pair;

public class Zigzag {

  private final ZigzagConfiguration configuration;
  private int currentLevel;
  private Set<InclusionDependency> satisfiedINDs;
  private Set<InclusionDependency> unsatisfiedINDs;
  private Set<InclusionDependency> unaryINDs;

  public Zigzag(ZigzagConfiguration configuration) {
    this.configuration = configuration;
    unaryINDs = calculateUnaryInclusionDependencies();
    currentLevel = configuration.getK();
  }

  public void execute() throws AlgorithmExecutionException {
    initialCandidateCheck(configuration.getK());

    // TODO when borders are updated always reduce to the most specialized/generalized INDs
    Set<InclusionDependency> positiveBorder = satisfiedINDs; // Bd+(I)
    Set<InclusionDependency> negativeBorder = unsatisfiedINDs; // Bd-(I)

    Set<InclusionDependency> optimisticBorder = calculateOptimisticBorder(new HashSet<>(unsatisfiedINDs)); // Bd+(I opt)

    while(!isOptimisticBorderFinal(optimisticBorder, positiveBorder)) {
      Set<InclusionDependency> possibleSmallerIND = new HashSet<>(); // optDI, all g3' < epsilon
      Set<InclusionDependency> pessDI = new HashSet<>(); // pessDI, all g3' > epsilon

      for(InclusionDependency ind : optimisticBorder) {
        int g3Val = g3(ind);
        if(g3Val == 0) {
          positiveBorder.add(ind);
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
      optimisticBorder = calculateOptimisticBorder(new HashSet<>(unsatisfiedINDs));
    }

  }

  private boolean isOptimisticBorderFinal(Set<InclusionDependency> optimisticBorder, Set<InclusionDependency> positiveBorder) {
    optimisticBorder.removeAll(positiveBorder);
    return optimisticBorder.isEmpty();
  }

  private Set<InclusionDependency> calculateOptimisticBorder(Set<InclusionDependency> unsatisfiedINDs) {
    Map<ColumnIdentifier, ColumnIdentifier> dependantToReferenced = convertUnaryINDsToMap(calculateUnaryInclusionDependencies());
    Set<InclusionDependency> solution = new HashSet<>();
    for (InclusionDependency head : unsatisfiedINDs) {
      List<ColumnIdentifier> dependants = head.getDependant().getColumnIdentifiers();
      List<ColumnIdentifier> referenced = head.getReferenced().getColumnIdentifiers();
      Set<InclusionDependency> headINDs = new HashSet<>();
      for (int i = 0; i < dependants.size(); i++) {
        headINDs.add(new InclusionDependency(new ColumnPermutation(dependants.get(i)), new ColumnPermutation(referenced.get(i))));
      }
      if(solution.size() == 0) {
        solution = headINDs;
      } else {
        Set<InclusionDependency> newSolution = new HashSet<>();
        for (InclusionDependency solIND : solution) {
          for (InclusionDependency headIND : headINDs) {
            if(headIND.equals(solIND)) {
              newSolution.add(headIND);
            } else {
              InclusionDependency combinedIND = combineINDs(headIND, solIND);
              newSolution.add(combinedIND);
            }
          }
        }
        solution = removeNonMinimalSets(newSolution);
      }
    }
    return solution.stream().map(this::invertIND).collect(Collectors.toSet());
  }

  private Map<ColumnIdentifier,ColumnIdentifier> convertUnaryINDsToMap(Set<InclusionDependency> unaryINDs) {
    Map<ColumnIdentifier,ColumnIdentifier> uINDs = new HashMap<>();
    for (InclusionDependency ind : unaryINDs) {
      for (int i = 0; i < ind.getDependant().getColumnIdentifiers().size(); i++) {
        uINDs.put(ind.getDependant().getColumnIdentifiers().get(i), ind.getReferenced().getColumnIdentifiers().get(i));
      }
    }
    return uINDs;
  }

  // Zigzag only needs to check one side for this, so use dependant
  private Set<InclusionDependency> removeNonMinimalSets(Set<InclusionDependency> solution) {
    Set<InclusionDependency> minimalSolution = new HashSet<>(solution);
    for (InclusionDependency ind1 : solution) {
      for (InclusionDependency ind2 : solution) {
        if(isSpecialization(ind1.getDependant(), ind2.getDependant())) {
          minimalSolution.remove(ind1);
        }
      }
    }
    return minimalSolution;
  }

  private InclusionDependency invertIND(InclusionDependency ind) {
    Map<ColumnIdentifier, ColumnIdentifier> dependantToReferenced = convertUnaryINDsToMap(calculateUnaryInclusionDependencies());
    System.out.println(dependantToReferenced);
    Set<ColumnIdentifier> uinds = new HashSet<>(dependantToReferenced.keySet());
    for(ColumnIdentifier depId : ind.getDependant().getColumnIdentifiers()) {
      uinds.remove(depId);
    }
    List<ColumnIdentifier> depList = new ArrayList<>();
    List<ColumnIdentifier> refList = new ArrayList<>();
    for (ColumnIdentifier depId : uinds) {
      System.out.println(depId);
      depList.add(depId);
      refList.add(dependantToReferenced.get(depId));
    }
    ColumnPermutation dep = new ColumnPermutation();
    dep.setColumnIdentifiers(depList);
    ColumnPermutation ref = new ColumnPermutation();
    ref.setColumnIdentifiers(refList);
    return new InclusionDependency(dep, ref);
  }

  private boolean isSpecialization(ColumnPermutation specialization, ColumnPermutation generalization) {
    if(specialization.getColumnIdentifiers().size() <= generalization.getColumnIdentifiers().size())
      return false;
    return specialization.getColumnIdentifiers().containsAll(generalization.getColumnIdentifiers());
  }

  private InclusionDependency combineINDs(InclusionDependency ind1, InclusionDependency ind2) {
    ColumnPermutation dep1 = ind1.getDependant();
    ColumnPermutation dep2 = ind2.getDependant();
    ColumnPermutation dep = new ColumnPermutation();
    List<ColumnIdentifier> dep1CI = new ArrayList<>(dep1.getColumnIdentifiers());
    dep1CI.addAll(dep2.getColumnIdentifiers());
    dep.setColumnIdentifiers(dep1CI);

    ColumnPermutation ref1 = ind1.getReferenced();
    ColumnPermutation ref2 = ind2.getReferenced();
    ColumnPermutation ref = new ColumnPermutation();
    List<ColumnIdentifier> ref1CI = new ArrayList<>(ref1.getColumnIdentifiers());
    ref1CI.addAll(ref2.getColumnIdentifiers());
    ref.setColumnIdentifiers(ref1CI);
    return new InclusionDependency(dep, ref);
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
    ColumnIdentifier a1 = new ColumnIdentifier("table", "a");
    ColumnIdentifier a2 = new ColumnIdentifier("table", "a2");
    ColumnIdentifier b1 = new ColumnIdentifier("table", "b");
    ColumnIdentifier b2 = new ColumnIdentifier("table", "b2");
    ColumnIdentifier c1 = new ColumnIdentifier("table", "c");
    ColumnIdentifier c2 = new ColumnIdentifier("table", "c2");
    ColumnIdentifier d1 = new ColumnIdentifier("table", "d");
    ColumnIdentifier d2 = new ColumnIdentifier("table", "d2");
    ColumnIdentifier e1 = new ColumnIdentifier("table", "e");
    ColumnIdentifier e2 = new ColumnIdentifier("table", "e2");

    Set<InclusionDependency> unaryINDs = new HashSet<>();
    unaryINDs.add(makeUnaryIND(a1,a2));
    unaryINDs.add(makeUnaryIND(b1,b2));
    unaryINDs.add(makeUnaryIND(c1,c2));
    unaryINDs.add(makeUnaryIND(d1,d2));
    unaryINDs.add(makeUnaryIND(e1,e2));
    System.out.println(unaryINDs);
    return unaryINDs;
  }

  private InclusionDependency makeUnaryIND(ColumnIdentifier dep, ColumnIdentifier ref) {
    return new InclusionDependency(new ColumnPermutation(dep), new ColumnPermutation(ref));
  }
}