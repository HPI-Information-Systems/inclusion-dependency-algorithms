package de.metanome.algorithms.find2;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.validation.ValidationStrategy;
import de.metanome.validation.ValidationStrategyFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

class FIND2 {

  private final FIND2Configuration config;
  private final ValidationStrategy validationStrategy;

  FIND2(FIND2Configuration config) {
    this.config = config;
    validationStrategy =
        new ValidationStrategyFactory().forDatabase(this.config.getValidationParameters());
  }

  void execute() throws AlgorithmConfigurationException, InputGenerationException {
    // System.out.println("Welcome to FIND2");
    // generate and validate unary and kary INDs;
    List<TableInputGenerator> tableInputGenerators = config.getTableInputGenerators();
    ArrayList<ColumnIdentifier> columnIdentifiers = getColumnIdentifiers(tableInputGenerators);
    HashSet<ExIND> unaries = getUnaryINDs(columnIdentifiers);
    HashSet<ExIND> karies = getKaryINDs(config.getStartK(), unaries);

    // add disconnected nodes to result set (unary-INDs which not support any kary-IND)
    HashSet<ExIND> karies1 = karies; // rename cause it has to be (effectively) final in lambda
    HashSet<ExIND> validINDs =
        unaries
            .stream()
            .filter(unary -> karies1.stream().noneMatch(kary -> kary.contains(unary)))
            .collect(Collectors.toCollection(HashSet::new));

    // remove disconnected nodes
    unaries.removeAll(validINDs);

    for (int k = config.getStartK(); k < unaries.size(); k++) {
      Hypergraph graph = new Hypergraph(k, unaries, karies);
      HashSet<ExIND> cliques = graph.getCliquesOfCurrentLevel();

      // cliques are IND candidates
      validateINDs(cliques);
      HashSet<ExIND> validINDCandidates = new HashSet<>();
      HashSet<ExIND> invalidINDCandidates = new HashSet<>();
      cliques.forEach(
          clique -> (clique.isValid() ? validINDCandidates : invalidINDCandidates).add(clique));
      validINDs.addAll(validINDCandidates);

      HashSet<ExIND> newKaries = getKaries(k + 1, invalidINDCandidates);
      validateINDs(newKaries);
      HashSet<ExIND> validNewKaries =
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

  /**
   * Iterates all TableInputGenerators and constructs all ColumnIdentifier found in RelationalInputs
   *
   * @param tableInputGenerators tableInputGenerators
   * @return constructed ColumnIdentifier
   */
  private ArrayList<ColumnIdentifier> getColumnIdentifiers(
      List<TableInputGenerator> tableInputGenerators)
      throws InputGenerationException, AlgorithmConfigurationException {
    ArrayList<ColumnIdentifier> columnIdentifier = new ArrayList<>();

    for (TableInputGenerator tig : tableInputGenerators) {
      RelationalInput ri = tig.generateNewCopy();
      String relation = ri.relationName();
      for (String column : ri.columnNames()) {
        columnIdentifier.add(new ColumnIdentifier(relation, column));
      }
    }
    return columnIdentifier;
  }

  /**
   * Enumerates all unary INDs and returns valid ones.
   *
   * @param columnIdentifiers all columnIdentifiers of input relations
   * @return valid unary INDs to be used as nodes in Hypergraph
   */
  private HashSet<ExIND> getUnaryINDs(List<ColumnIdentifier> columnIdentifiers)
      throws InputGenerationException, AlgorithmConfigurationException {
    HashSet<ExIND> unaries = new HashSet<>();

    for (int i = 0; i < columnIdentifiers.size() - 1; i++)
      for (int j = i + 1; j < columnIdentifiers.size(); j++)
        unaries.add(
            new ExIND(
                new ColumnPermutation(columnIdentifiers.get(i)),
                new ColumnPermutation(columnIdentifiers.get(j))));

    validateINDs(unaries);
    return unaries.stream().filter(ExIND::isValid).collect(Collectors.toCollection(HashSet::new));
  }

  /**
   * Generates all kary-INDs at level startK from given unary-INDs and returns valid ones.
   *
   * @param startK k in k-ary
   * @param unaries (valid) unary-INDs
   * @return valid kary-INDs
   */
  private HashSet<ExIND> getKaryINDs(int startK, HashSet<ExIND> unaries) {
    ArrayList<ExIND> karyList = new ArrayList<>();
    ArrayList<ExIND> unaryList = new ArrayList<>(unaries);
    getSubset(startK, unaryList, 0, new HashSet<>(), karyList);

    HashSet<ExIND> karies = new HashSet<>(karyList);

    // Remove kary-INDs, which are spanning over multiple tables.
    // Respectively, dependant and references side have to be from one table.
    karies.removeIf(
        kary ->
            kary.getDependant()
                        .getColumnIdentifiers()
                        .stream()
                        .map(ColumnIdentifier::getTableIdentifier)
                        .collect(Collectors.toCollection(HashSet::new))
                        .size()
                    != 1
                || kary.getReferenced()
                        .getColumnIdentifiers()
                        .stream()
                        .map(ColumnIdentifier::getTableIdentifier)
                        .collect(Collectors.toCollection(HashSet::new))
                        .size()
                    != 1);

    validateINDs(karies);
    return karies.stream().filter(ExIND::isValid).collect(Collectors.toCollection(HashSet::new));
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
  private HashSet<ExIND> genSubINDs(
      HashSet<ExIND> validKaries, HashSet<ExIND> validNewKaries, HashSet<ExIND> validINDs) {
    return validKaries
        .stream()
        .filter(
            kary ->
                validINDs.stream().noneMatch(IND -> IND.contains(kary))
                    && validNewKaries.stream().noneMatch(newKary -> newKary.contains(kary)))
        .collect(Collectors.toCollection(HashSet::new));
  }

  private HashSet<ExIND> getKaries(int k, HashSet<ExIND> invalidINDs) {
    HashSet<ExIND> allNewKaries = new HashSet<>();
    for (ExIND kary : invalidINDs) {
      ArrayList<ExIND> superSet = new ArrayList<>(kary.getAllUnaries());
      ArrayList<ExIND> newKaries = new ArrayList<>();
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
   *     Main object in recursion.
   * @param solution list of all kary INDs
   */
  private void getSubset(
      int k, ArrayList<ExIND> superSet, int ix, HashSet<ExIND> current, ArrayList<ExIND> solution) {
    if (current.size() == k) {
      solution.add(ExIND.toExIND(current));
      return;
    }
    if (ix == superSet.size()) return;

    ExIND IND = superSet.get(ix);
    current.add(IND);
    getSubset(k, superSet, ix + 1, current, solution);
    current.remove(IND);
    getSubset(k, superSet, ix + 1, current, solution);
  }

  private void validateINDs(HashSet<ExIND> INDCandidates) {
    for (ExIND INDCandidate : INDCandidates)
      if (INDCandidate.isValid() == null)
        INDCandidate.setValidity(validationStrategy.validate(INDCandidate).isValid());
  }

  private void returnResults(HashSet<ExIND> validINDs) {
    for (ExIND IND : validINDs)
      try {
        config.getResultReceiver().receiveResult(IND);
      } catch (CouldNotReceiveResultException | ColumnNameMismatchException e) {
        e.printStackTrace();
      }
  }
}
