package de.metanome.algorithms.mind;

import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;


@Data
@Builder
class Configuration {

  private final InclusionDependencyResultReceiver resultReceiver;

  @Singular
  private final List<TableInputGenerator> tableInputGenerators;

  private final DatabaseConnectionGenerator databaseConnectionGenerator;

}
