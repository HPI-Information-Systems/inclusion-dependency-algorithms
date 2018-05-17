package de.metanome.algorithms.spiderbruteforce;

import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.util.TPMMSConfiguration;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Singular;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class Configuration {

  @Singular
  private List<RelationalInputGenerator> relationalInputGenerators;

  private boolean processEmptyColumns;
  private TPMMSConfiguration tpmmsConfiguration;

  private FileGenerator tempFileGenerator;
  private InclusionDependencyResultReceiver resultReceiver;

  static Configuration withDefaults() {
    return builder()
        .relationalInputGenerators(Collections.emptyList())
        .tempFileGenerator(null)
        .resultReceiver(null)
        .processEmptyColumns(true)
        .tpmmsConfiguration(TPMMSConfiguration.withDefaults())
        .build();
  }

}
