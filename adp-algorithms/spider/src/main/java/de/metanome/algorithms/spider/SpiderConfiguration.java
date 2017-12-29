package de.metanome.algorithms.spider;

import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Singular;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SpiderConfiguration {

  private FileGenerator tempFileGenerator;
  private InclusionDependencyResultReceiver resultReceiver;

  private int inputRowLimit;
  private int maxMemoryUsage;
  private int memoryCheckInterval;

  @Singular
  private List<TableInputGenerator> tableInputGenerators;
  @Singular
  private List<RelationalInputGenerator> relationalInputGenerators;

  public static SpiderConfiguration withDefaults() {
    return builder()
        .resultReceiver(null)
        .inputRowLimit(-1)
        .maxMemoryUsage(2048 * 10214)
        .memoryCheckInterval(500)
        .tableInputGenerators(Collections.emptyList())
        .relationalInputGenerators(Collections.emptyList())
        .build();
  }
}
