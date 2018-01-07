package de.metanome.algorithms.binder;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

@Data
@Builder
class BinderConfiguration {

//    private final String temporaryFolderPath;
//    private final boolean clearTemporaryFolder;
    private final InclusionDependencyResultReceiver resultReceiver;

    //private final detectNary;

//    private final int inputRowLimit;
//    private final long maxMemoryUsage;
//    private final int memoryCheckInterval;
//    private final int maxNaryLevel;

    @Singular
    public final List<TableInputGenerator> tableInputGenerators;
    @Singular
    public final List<RelationalInputGenerator> relationalInputGenerators;
}
