package de.metanome.algorithms.zigzag;

import static de.metanome.algorithms.zigzag.configuration.ConfigurationKey.TABLE;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementTableInput;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithms.zigzag.configuration.ZigzagConfiguration;
import de.metanome.algorithms.zigzag.configuration.ZigzagConfiguration.ZigzagConfigurationBuilder;
import de.metanome.input.ind.InclusionDependencyInputConfigurationRequirements;
import de.metanome.validation.ValidationConfigurationRequirements;
import java.util.ArrayList;
import java.util.List;

public class ZigzagDatabaseAlgorithm extends ZigzagAlgorithm implements TableInputParameterAlgorithm {

    private final ZigzagConfigurationBuilder configurationBuilder;

    public ZigzagDatabaseAlgorithm() {
        configurationBuilder = ZigzagConfiguration.builder();
    }

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
        requirements.addAll(common());
        requirements.addAll(database());
        return requirements;
    }

    private List<ConfigurationRequirement<?>> database() {
        final List<ConfigurationRequirement<?>> requirements = new ArrayList<>();
        requirements.add(new ConfigurationRequirementTableInput(TABLE.name()));
        return requirements;
    }

    @Override
    public void setTableInputConfigurationValue(String identifier, TableInputGenerator... values) {
        if (identifier.equals(TABLE.name())) {
            configurationBuilder.tableInputGenerator(values[0]);
            InclusionDependencyInputConfigurationRequirements
                    .acceptTableInputGenerator(values, indInputParams);
            DatabaseConnectionGenerator[] databaseConnectionGenerators = {
                    values[0].getDatabaseConnectionGenerator()};
            ValidationConfigurationRequirements
                    .acceptDatabaseConnectionGenerator(databaseConnectionGenerators,
                            validationParameters);
        }
    }
}