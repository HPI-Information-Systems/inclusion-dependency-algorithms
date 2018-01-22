package de.metanome.algorithms.sindd;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.util.TPMMSConfiguration;
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

  private final boolean processEmptyColumns;

  private final TPMMSConfiguration tpmmsConfiguration;

  public static Configuration withDefaults() {
    return builder()
        .resultReceiver(null)
        .relationalInputGenerators(Collections.emptyList())
        .tableInputGenerators(Collections.emptyList())
        .openFileNr(100)
        .partitionNr(1)
        .processEmptyColumns(true)
        .tpmmsConfiguration(TPMMSConfiguration.withDefaults())
        .build();
  }
}
