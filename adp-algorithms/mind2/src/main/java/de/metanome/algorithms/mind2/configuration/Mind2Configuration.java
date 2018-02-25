package de.metanome.algorithms.mind2.configuration;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithms.mind2.utils.DataAccessObject;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Mind2Configuration {

    private final InclusionDependencyResultReceiver resultReceiver;
    private final ImmutableList<TableInputGenerator> inputGenerators;
    private final FileGenerator tempFileGenerator;
    private final DataAccessObject dataAccessObject;
    private final String indexColumn = "mind2index";
}
