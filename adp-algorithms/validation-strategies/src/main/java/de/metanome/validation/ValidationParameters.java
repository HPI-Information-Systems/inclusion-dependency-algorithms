package de.metanome.validation;

import de.metanome.validation.database.DatabaseValidationParameters;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Delegate;


@NoArgsConstructor
@AllArgsConstructor
@Data
public class ValidationParameters {

  @Delegate
  private DatabaseValidationParameters databaseParameters = new DatabaseValidationParameters();
}