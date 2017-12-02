package de.metanome.algorithms.bellbrockhausen.configuration;

import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BellBrockhausenConfiguration {

    private final InclusionDependencyResultReceiver resultReceiver;
    private final DatabaseConnectionGenerator connectionGenerator;
    private final String tableName;
}
