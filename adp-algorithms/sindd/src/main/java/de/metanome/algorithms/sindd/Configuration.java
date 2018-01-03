package de.metanome.algorithms.sindd;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

@Data
@Builder
public class Configuration {

  private final InclusionDependencyResultReceiver resultReceiver;

  @Singular
  private final List<RelationalInputGenerator> relationalInputGenerators;
  @Singular
  private final List<TableInputGenerator> tableInputGenerators;

  private final int openFileNr;
  private final int partitionNr;

  private final int inputRowLimit;
  private final int maxMemoryUsage;
  private final int memoryCheckInterval;

  public static Configuration withDefaults() {
    return builder()
        .resultReceiver(null)
        .relationalInputGenerators(Collections.emptyList())
        .tableInputGenerators(Collections.emptyList())
        .openFileNr(100)
        .partitionNr(1)
        .inputRowLimit(-1)
        .maxMemoryUsage(2048 * 10214)
        .memoryCheckInterval(500)
        .build();
  }
}
