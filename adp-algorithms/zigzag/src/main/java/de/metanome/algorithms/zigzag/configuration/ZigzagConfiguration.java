package de.metanome.algorithms.zigzag.configuration;

import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ZigzagConfiguration {

    private final InclusionDependencyResultReceiver resultReceiver;
    private final TableInputGenerator tableInputGenerator;
    private final Integer k;
    private final Integer epsilon;
}
