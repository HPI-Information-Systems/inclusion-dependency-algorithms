package de.metanome.validation.database;

import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class DatabaseValidationParameters {

  private QueryType queryType;
  private DatabaseConnectionGenerator connectionGenerator;
}
