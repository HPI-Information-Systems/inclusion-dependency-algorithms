package de.metanome.algorithms.mind2.configuration;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Mind2Configuration {

    private final InclusionDependencyResultReceiver resultReceiver;
    private final ImmutableList<RelationalInputGenerator> relationalInputGenerators;
}
