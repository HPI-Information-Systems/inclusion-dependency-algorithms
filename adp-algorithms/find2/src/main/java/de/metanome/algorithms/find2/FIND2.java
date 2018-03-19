package de.metanome.algorithms.find2;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.input.ind.InclusionDependencyInput;
import de.metanome.input.ind.InclusionDependencyInputGenerator;
import de.metanome.validation.ValidationStrategy;
import de.metanome.validation.ValidationStrategyFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.stream.Collectors;

class FIND2 {

  private final static Logger log = Logger.getLogger(FIND2.class.getName());

  private final FIND2Configuration config;
  private final ValidationStrategy validationStrategy;
  private final InclusionDependencyInput inclusionDependencyInput;

  FIND2(FIND2Configuration config) {
    log.setLevel(Level.FINE);
    this.config = config;
    validationStrategy =
        new ValidationStrategyFactory().forDatabase(this.config.getValidationParameters());
    inclusionDependencyInput = new InclusionDependencyInputGenerator()
        .get(config.getInclusionDependencyParameters());
  }

  void execute() throws AlgorithmExecutionException {
    log.fine("Start of algorithm FIND2.");
    // generate and validate unary and kary INDs;
    List<InclusionDependency> inputInds = inclusionDependencyInput.execute();
    Set<ExIND> unaries = getUnaryINDs(inputInds);
    Set<ExIND> karies = getKaryINDs(config.getStartK(), inputInds);

    // add disconnected nodes to result set (unary-INDs which not support any kary-IND)
    Set<ExIND> karies1 = karies; // rename cause it has to be (effectively) final in lambda
    Set<ExIND> validINDs =
        unaries
            .stream()
            .filter(unary -> karies1.stream().noneMatch(kary -> kary.contains(unary)))
            .collect(Collectors.toCollection(HashSet::new));

    // remove disconnected nodes
    unaries.removeAll(validINDs);

    for (int k = config.getStartK(); k < unaries.size(); k++) {
      Hypergraph graph = new Hypergraph(k, unaries, karies);
      Set<ExIND> cliques = graph.getCliquesOfCurrentLevel();

      // cliques are IND candidates
      validateINDs(cliques);
      Set<ExIND> validINDCandidates = new HashSet<>();
      Set<ExIND> invalidINDCandidates = new HashSet<>();
      cliques.forEach(
          clique -> (clique.isValid() ? validINDCandidates : invalidINDCandidates).add(clique));
      validINDs.addAll(validINDCandidates);

      Set<ExIND> newKaries = getKaries(k + 1, invalidINDCandidates);
      validateINDs(newKaries);
      Set<ExIND> validNewKaries =
          newKaries.stream().filter(ExIND::isValid).collect(Collectors.toCollection(HashSet::new));
      validINDs.addAll(genSubINDs(karies, validNewKaries, validINDs));

      if (validNewKaries.size() < k + 1) {
        validINDs.addAll(validNewKaries);
        returnResults(validINDs);
        return;
      }

      karies = validNewKaries;
    }
  }

  private Set<ExIND> getUnaryINDs(final Collection<InclusionDependency> inputINDs) {
    final Set<ExIND> result = new HashSet<>();
    for (InclusionDependency ind : inputINDs) {
      if (ind.getDependant().getColumnIdentifiers().size() == 1) {
        final ExIND exIND = new ExIND(ind.getDependant(), ind.getReferenced());
        exIND.setValidity(true);
        result.add(exIND);
      }
    }
    return result;
  }

  /**
   * Generates all kary-INDs at level startK from given unary-INDs and returns valid ones.
   *
   * @param startK k in k-ary
   * @param inputINDs valid input INDs
   * @return valid kary-INDs
   */
  private Set<ExIND> getKaryINDs(int startK, Collection<InclusionDependency> inputINDs) {
    final Set<ExIND> result = new HashSet<>();
    for (final InclusionDependency inputInd : inputINDs) {
      if (inputInd.getDependant().getColumnIdentifiers().size() == startK) {
        final ExIND exIND = new ExIND(inputInd.getDependant(), inputInd.getReferenced());
        exIND.setValidity(true);
        result.add(exIND);
      }
    }
    return result;
  }

  /**
   * Finds all kary INDs (of current level), which are not in the result set by now, but also not in
   * any new (valid) k+1ary-IND. Then, those have to be added to the result set.
   *
   * @param validKaries valid kary-INDs of current level
   * @param validNewKaries valid k+1ary-INDs
   * @param validINDs current result set
   * @return kary-INDs of current level to be inserted into result set
   */
  private Set<ExIND> genSubINDs(Set<ExIND> validKaries, Set<ExIND> validNewKaries,
      Set<ExIND> validINDs) {

    return validKaries
        .stream()
        .filter(
            kary ->
                validINDs.stream().noneMatch(IND -> IND.contains(kary))
                    && validNewKaries.stream().noneMatch(newKary -> newKary.contains(kary)))
        .collect(Collectors.toCollection(HashSet::new));
  }

  private Set<ExIND> getKaries(int k, Set<ExIND> invalidINDs) {
    Set<ExIND> allNewKaries = new HashSet<>();
    for (ExIND kary : invalidINDs) {
      List<ExIND> superSet = new ArrayList<>(kary.getAllUnaries());
      List<ExIND> newKaries = new ArrayList<>();
      getSubset(k, superSet, 0, new HashSet<>(), newKaries);
      allNewKaries.addAll(newKaries);
    }
    return allNewKaries;
  }

  /**
   * Finds all subsets of size k in an set of all unary-INDs of a kary-IND. Solution is list of
   * ExINDs constructed from those subsets.
   *
   * <p>Recursion: Enumerate all possible subsets. Break on size k.
   *
   * @param k k
   * @param superSet list of all unary-INDs
   * @param ix current index. Indicates which unary is added and not added next.
   * @param current subset constructed until now. The next unary will be 1. added and 2. not added.
   * Main object in recursion.
   * @param solution list of all kary INDs
   */
  private void getSubset(
      int k, List<ExIND> superSet, int ix, Set<ExIND> current, List<ExIND> solution) {
    if (current.size() == k) {
      solution.add(ExIND.toExIND(current));
      return;
    }
    if (ix == superSet.size()) {
      return;
    }

    ExIND IND = superSet.get(ix);
    current.add(IND);
    getSubset(k, superSet, ix + 1, current, solution);
    current.remove(IND);
    getSubset(k, superSet, ix + 1, current, solution);
  }

  private void validateINDs(Set<ExIND> INDCandidates) {
    for (ExIND INDCandidate : INDCandidates) {
      if (INDCandidate.isValid() == null) {
        INDCandidate.setValidity(validationStrategy.validate(INDCandidate).isValid());
      }
    }
  }

  private void returnResults(Set<ExIND> validINDs) throws AlgorithmExecutionException {
    for (ExIND IND : validINDs) {
      config.getResultReceiver().receiveResult(IND);
    }
  }
}
