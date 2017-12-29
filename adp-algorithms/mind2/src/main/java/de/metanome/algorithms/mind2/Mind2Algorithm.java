package de.metanome.algorithms.mind2;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithms.mind2.configuration.ConfigurationKey;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration.Mind2ConfigurationBuilder;

import java.util.ArrayList;

public class Mind2Algorithm implements InclusionDependencyAlgorithm, TableInputParameterAlgorithm {

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
    public void setTableInputConfigurationValue(String identifier, TableInputGenerator... values) {
        if (identifier.equals(ConfigurationKey.TABLE.name()) && values.length > 0) {
            configurationBuilder.inputGenerators(ImmutableList.copyOf(values));
        }
    }

    @Override
    public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
        configurationBuilder.resultReceiver(resultReceiver);
    }

    @Override
    public void execute() throws AlgorithmExecutionException {
        Mind2 mind2 = new Mind2(configurationBuilder.build());
        mind2.execute();
    }

    @Override
    public String getAuthors() {
        return "Nils Strelow, Fabian Windheuser";
    }

    @Override
    public String getDescription() {
        return "Implementation of 'Detecting Maximum Inclusion Dependencies without Candidate Generation' " +
                "by Shaabani, Meinel, 2016";
    }
}
