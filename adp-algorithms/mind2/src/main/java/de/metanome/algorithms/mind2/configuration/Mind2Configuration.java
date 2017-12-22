package de.metanome.algorithms.mind2.configuration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Mind2Configuration {

    private final InclusionDependencyResultReceiver resultReceiver;
    private final ImmutableList<RelationalInputGenerator> relationalInputGenerators;
    private final ImmutableSet<InclusionDependency> unaryInds;
    private final FileGenerator tempFileGenerator;
}
