package de.metanome.algorithms.sindd;

import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

import java.util.List;

@Data
@Builder
public class Configuration {

  private final InclusionDependencyResultReceiver resultReceiver;

  @Singular
  private final List<TableInputGenerator> tableInputGenerators;

  private final DatabaseConnectionGenerator databaseConnectionGenerator;

  private final int openFileNr;

  private final int partitionNr;

}
