package de.metanome.algorithms.mind;


import de.metanome.algorithms.demarchi.DeMarchiAlgorithm;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.backend.input.database.DefaultTableInputGeneratorWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;


class Mind {

  private Configuration configuration;
  protected List<String> relationNames;

  protected List<TableInfo> tables;

  void execute(final Configuration configuration) throws AlgorithmExecutionException{
    this.configuration = configuration;
    initialize();
    List<ColumnPermutation[]> candidates = genLevel1Candidates();

    int depth = 1;
    while(candidates.size() > 0) {

      List<ColumnPermutation[]> inds = genLevel1Candidates();
      for (ColumnPermutation[] candidate : candidates) {
        ColumnPermutation lhs = candidate[0];
        ColumnPermutation rhs = candidate[1];
        if (isInd(lhs, rhs, depth)) {
          InclusionDependency ind = new InclusionDependency(lhs, rhs);
          configuration.getResultReceiver().receiveResult(ind);
          inds.add(candidate);
        }
      }
      depth++;
      candidates = genNextLevelCandidates(inds);
    }
  }

  private List<ColumnPermutation[]> genLevel1Candidates(){
    List<ColumnIdentifier> attributes = new ArrayList<ColumnIdentifier>();
    List<ColumnPermutation[]> candidates = new ArrayList<ColumnPermutation[]>();
    for (final TableInfo table : this.tables) {
      for(String column : table.getColumnNames()){
        attributes.add(new ColumnIdentifier(table.getTableName(), column));
      }
    }

    for(ColumnIdentifier lhs : attributes){
      for(ColumnIdentifier rhs : attributes){
        if(lhs != rhs) {
          ColumnPermutation[] candidate = {new ColumnPermutation(lhs), new ColumnPermutation(rhs)};
          candidates.add(candidate);
        }
      }
    }

    return candidates;
  }

  // TODO ensure that no duplicates are created.
  private List<ColumnPermutation[]> genNextLevelCandidates(List<ColumnPermutation[]> previous) {
    List<ColumnPermutation[]> candidates = new ArrayList<ColumnPermutation[]>();
    for(int index1 = 0; index1 < previous.size(); index1++){
      for(int index2 = 0; index2 < previous.size(); index2++){

        if(samePrefix(previous.get(index1)[0], previous.get(index2)[0]) &&
            samePrefix(previous.get(index1)[1], previous.get(index2)[1]) &&
            sameTable(previous.get(index1), previous.get(index2))){

          ColumnPermutation[] candidate = {
              new ColumnPermutation(),
              new ColumnPermutation() };
          List<ColumnIdentifier> colIdsLHS = new ArrayList<ColumnIdentifier>();
          List<ColumnIdentifier> colIdsRHS = new ArrayList<ColumnIdentifier>();
          for(ColumnIdentifier colId : previous.get(index1)[0].getColumnIdentifiers()){
            colIdsLHS.add(colId);
          }
          List<ColumnIdentifier> index2Ids = previous.get(index2)[0].getColumnIdentifiers();
          colIdsLHS.add(index2Ids.get(index2Ids.size()-1));

          for(ColumnIdentifier colId : previous.get(index1)[1].getColumnIdentifiers()){
            colIdsRHS.add(colId);
          }

          index2Ids = previous.get(index2)[1].getColumnIdentifiers();
          colIdsRHS.add(index2Ids.get(index2Ids.size()-1));

          candidate[0].setColumnIdentifiers(colIdsLHS);
          candidate[1].setColumnIdentifiers(colIdsRHS);
          if(notToPrune(candidate) && isNotDoublon(candidate)){
            System.out.println(candidate[0].toString() +" -> "+ candidate[1].toString());
            candidates.add(candidate);
          }
        }
      }
    }
    return candidates;
  }

  // TODO implement
  private boolean notToPrune(ColumnPermutation[] candidate){
    return true;
  }

  private boolean isNotDoublon(ColumnPermutation[] candidate){
    List<ColumnIdentifier> colIdsLHS = candidate[0].getColumnIdentifiers();
    List<ColumnIdentifier> colIdsRHS = candidate[1].getColumnIdentifiers();
    for(ColumnIdentifier colId : colIdsLHS){
      if(Collections.frequency(colIdsLHS, colId) > 1 || Collections.frequency(colIdsRHS, colId) > 0){
        return false;
      }
    }
    for(ColumnIdentifier colId : colIdsRHS){
      if(Collections.frequency(colIdsLHS, colId) > 0 || Collections.frequency(colIdsRHS, colId) > 1){
        return false;
      }
    }
    return true;
  }

  private boolean sameTable(ColumnPermutation[] columnPermutation1, ColumnPermutation[] columnPermutation2){
    return (columnPermutation1[0].getColumnIdentifiers().get(0).getTableIdentifier().equals(
            columnPermutation2[0].getColumnIdentifiers().get(0).getTableIdentifier())) &&
            (columnPermutation1[1].getColumnIdentifiers().get(0).getTableIdentifier().equals(
            columnPermutation2[1].getColumnIdentifiers().get(0).getTableIdentifier()));
  }

  private boolean samePrefix(ColumnPermutation columnPermutation1, ColumnPermutation columnPermutation2){
    List<ColumnIdentifier> col1Identifiers = columnPermutation1.getColumnIdentifiers();
    List<ColumnIdentifier> col2Identifiers = columnPermutation2.getColumnIdentifiers();

    for(int index = 0; index < col1Identifiers.size()-2; index++){
      if(col1Identifiers.get(index) != col2Identifiers.get(index)){
        return false;
      }
    }
    return true;
  }

  private void initialize() throws InputGenerationException, AlgorithmConfigurationException{
    this.relationNames = new ArrayList<String>();
    TableInfoFactory tableInfoFactory = new TableInfoFactory();

    this.tables = tableInfoFactory
        .createFromTableInputs(configuration.getTableInputGenerators());

    for (final TableInfo table : tables) {
      this.relationNames.add(table.getTableName());
    }
  }

  private boolean isInd(ColumnPermutation lhs, ColumnPermutation rhs, int depth) throws InputGenerationException, AlgorithmConfigurationException{

    try{
      String query = "";
      if(depth == 1){
        query = indTestQueryNotIn(lhs, rhs);
      } else{
        query = indTestQueryNotExists(lhs, rhs);
      }
      ResultSet result = configuration.getDatabaseConnectionGenerator().generateResultSetFromSql(query);
      result.next();

      return result.getInt("result") == 0;

    }catch(SQLException se){
      //Handle errors for JDBC
      se.printStackTrace();
    }catch(Exception e){
      //Handle errors for Class.forName
      e.printStackTrace();
    }
    return false;
  }

  // select count(*) as result from (SELECT test.a FROM test where test.a NOT IN (SELECT test.c FROM test)) as d
  // Does not work with mysql for n-ary inds
  private String indTestQueryNotIn(ColumnPermutation lhs, ColumnPermutation rhs){
    String strQuery =  "SELECT count(*) as result FROM ";
    strQuery += "(SELECT ";
    strQuery += lhs.getColumnIdentifiers().stream()
        .map(ColumnIdentifier::toString)
        .collect(Collectors.joining(", "));
    strQuery += " FROM ";
    //strQuery += lhs.getColumnIdentifiers().stream()
    //    .map(ColumnIdentifier::getTableIdentifier)
    //    .collect(Collectors.joining(", "));
    strQuery += this.relationNames.stream()
        .collect(Collectors.joining(", "));
    strQuery += " WHERE ( ";
    strQuery += lhs.getColumnIdentifiers().stream()
        .map(ColumnIdentifier::toString)
        .collect(Collectors.joining(" IS NOT NULL AND "));
    strQuery += " IS NOT NULL) AND ";
    strQuery += lhs.getColumnIdentifiers().stream()
        .map(ColumnIdentifier::toString)
        .collect(Collectors.joining(", "));
    strQuery += " NOT IN (SELECT ";
    strQuery += rhs.getColumnIdentifiers().stream()
        .map(ColumnIdentifier::toString)
        .collect(Collectors.joining(", "));
    strQuery += " FROM ";
    strQuery += this.relationNames.stream()
        .collect(Collectors.joining(", "));
    strQuery += ")) as indCheck";

    return strQuery;
  }

  // SELECT * From test, test_copy where test.a is not null and test.c and test_copy.a is not null and test_copy.c is not null and
  // not exists (
  // SELECT test.a, test.b, test_copy.a, test_copy.b FROM test WHERE test.a = test_copy.a and test.b = test_copy.b) LIMIT 1
  // select count(*) as result from (SELECT test.a FROM test where test.a NOT IN (SELECT test.c FROM test)) as d
  // Does not work with mysql for unary-ary inds.
  private String indTestQueryNotExists(ColumnPermutation lhs, ColumnPermutation rhs){
    String strQuery =  "SELECT count(*) as result FROM ";
    strQuery += this.relationNames.stream()
        .collect(Collectors.joining(", "));
    strQuery += " WHERE ";
    strQuery += lhs.getColumnIdentifiers().stream()
        .map(ColumnIdentifier::toString)
        .collect(Collectors.joining(" IS NOT NULL AND "));
    strQuery += " IS NOT NULL AND NOT EXISTS (SELECT ";
    strQuery += lhs.getColumnIdentifiers().stream()
        .map(ColumnIdentifier::toString)
        .collect(Collectors.joining(", "));
    strQuery += ", ";
    strQuery += rhs.getColumnIdentifiers().stream()
        .map(ColumnIdentifier::toString)
        .collect(Collectors.joining(", "));
    strQuery += " FROM ";
    strQuery += this.relationNames.stream()
        .collect(Collectors.joining(", "));
    strQuery += " WHERE ( ";
    List<String> tuples = new ArrayList<String>();
    for(int index=0; index < lhs.getColumnIdentifiers().size(); index++){
      tuples.add(lhs.getColumnIdentifiers().get(index).toString() +" = "+ rhs.getColumnIdentifiers().get(index).toString());
    }
    strQuery += tuples.stream()
        .collect(Collectors.joining(" AND "));
    strQuery += ")) LIMIT 1";
    return strQuery;
  }

}
