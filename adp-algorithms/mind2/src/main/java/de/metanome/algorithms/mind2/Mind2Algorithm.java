package de.metanome.algorithms.mind2;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithms.mind2.configuration.ConfigurationKey;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration.Mind2ConfigurationBuilder;

import java.util.ArrayList;

public class Mind2Algorithm implements InclusionDependencyAlgorithm, RelationalInputParameterAlgorithm {

    private final Mind2ConfigurationBuilder configurationBuilder;

    public Mind2Algorithm() {
       configurationBuilder = Mind2Configuration.builder();
    }

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
        requirements.add(new ConfigurationRequirementRelationalInput(
                ConfigurationKey.TABLE.name(), ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES));
        return requirements;
    }

    @Override
    public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values)
            throws AlgorithmConfigurationException {
        if (identifier.equals(ConfigurationKey.TABLE.name()) && values.length > 0) {
            configurationBuilder.relationalInputGenerators(ImmutableList.copyOf(values));
        }
    }

    @Override
    public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
        configurationBuilder.resultReceiver(resultReceiver);
    }

    @Override
    public void execute() throws AlgorithmExecutionException {
        Mind2Configuration configuration = configurationBuilder.build();
        // TODO: Execute algorithm
    }

    @Override
    public String getAuthors() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }
}