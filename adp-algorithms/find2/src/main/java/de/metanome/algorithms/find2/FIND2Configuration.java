package de.metanome.algorithms.find2;

import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.validation.ValidationParameters;
// import java.util.HashSet;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

@Builder
@Data
class FIND2Configuration {
  private final InclusionDependencyResultReceiver resultReceiver;

  @Singular private final List<TableInputGenerator> tableInputGenerators;
  private final DatabaseConnectionGenerator databaseConnectionGenerator;

  // private final HashSet<ExIND> unaryINDs;
  // private final HashSet<ExIND> karyINDs;
  private final ValidationParameters validationParameters;
  private final int startK;
}
