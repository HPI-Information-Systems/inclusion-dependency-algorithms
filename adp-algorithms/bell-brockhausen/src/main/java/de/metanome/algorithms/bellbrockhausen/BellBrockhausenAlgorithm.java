package de.metanome.algorithms.bellbrockhausen;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.DatabaseConnectionParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.StringParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementDatabaseConnection;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementString;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithms.bellbrockhausen.accessors.DataAccessObject;
import de.metanome.algorithms.bellbrockhausen.accessors.PostgresDataAccessObject;
import de.metanome.algorithms.bellbrockhausen.configuration.BellBrockhausenConfiguration;
import de.metanome.algorithms.bellbrockhausen.configuration.BellBrockhausenConfiguration.BellBrockhausenConfigurationBuilder;

import java.util.ArrayList;

import static de.metanome.algorithms.bellbrockhausen.configuration.ConfigurationKey.DATABASE;
import static de.metanome.algorithms.bellbrockhausen.configuration.ConfigurationKey.TABLE;

public class BellBrockhausenAlgorithm implements InclusionDependencyAlgorithm,
        DatabaseConnectionParameterAlgorithm, StringParameterAlgorithm {

    private final BellBrockhausenConfigurationBuilder configurationBuilder;

    public BellBrockhausenAlgorithm() {
        configurationBuilder = BellBrockhausenConfiguration.builder();
    }

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
        requirements.add(new ConfigurationRequirementDatabaseConnection(DATABASE.name()));
        requirements.add(new ConfigurationRequirementString(TABLE.name()));
        return requirements;
    }

    @Override
    public void setDatabaseConnectionGeneratorConfigurationValue(
            String identifier, DatabaseConnectionGenerator... values) throws AlgorithmConfigurationException {
        if (identifier.equals(DATABASE.name())) {
            configurationBuilder.connectionGenerator(values[0]);
        }
    }

    @Override
    public void setStringConfigurationValue(String identifier, String... values) throws AlgorithmConfigurationException {
        if (identifier.equals(TABLE.name())) {
            configurationBuilder.tableName(values[0]);
        }
    }

    @Override
    public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
        configurationBuilder.resultReceiver(resultReceiver);
    }

    @Override
    public void execute() throws AlgorithmExecutionException {
        BellBrockhausenConfiguration configuration = configurationBuilder.build();
        DataAccessObject dataAccessObject = new PostgresDataAccessObject(configuration.getConnectionGenerator());
        BellBrockhausen algorithm = new BellBrockhausen(configuration, dataAccessObject);
        algorithm.execute();
    }

    @Override
    public String getAuthors() {
        return null;
    }

    @Override
    public String getDescription() {
        return "Implementation of 'Discovery of Data Dependencies in Relational Databases' by Bell, Brockhausen, 1995";
    }
}
