package de.metanome.algorithms.bellbrockhausen;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.DatabaseConnectionParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementDatabaseConnection;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;

import java.util.ArrayList;

public class BellBrockhausenAlgorithm implements InclusionDependencyAlgorithm, DatabaseConnectionParameterAlgorithm {

    @Override
    public void setDatabaseConnectionGeneratorConfigurationValue(
            String identifier, DatabaseConnectionGenerator... values) throws AlgorithmConfigurationException {

    }

    @Override
    public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {

    }

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
        requirements.add(new ConfigurationRequirementDatabaseConnection(ConfigurationKey.DATABASE.name()));
        return requirements;
    }

    @Override
    public void execute() throws AlgorithmExecutionException {

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
