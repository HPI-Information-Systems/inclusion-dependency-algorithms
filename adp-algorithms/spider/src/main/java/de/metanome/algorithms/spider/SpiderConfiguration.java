package de.metanome.algorithms.spider;

import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

@Data
@Builder
class SpiderConfiguration {

  private final FileGenerator tempFileGenerator;
  private final InclusionDependencyResultReceiver resultReceiver;

  private final int inputRowLimit;
  private final long maxMemoryUsage;
  private final int memoryCheckInterval;

  @Singular
  private final List<TableInputGenerator> tableInputGenerators;
  @Singular
  private final List<RelationalInputGenerator> relationalInputGenerators;
}
