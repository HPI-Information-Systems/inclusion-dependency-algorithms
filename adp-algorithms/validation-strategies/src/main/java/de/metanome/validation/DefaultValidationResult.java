package de.metanome.validation;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class DefaultValidationResult implements ValidationResult {

  private final boolean valid;

  @Override
  public boolean isValid() {
    return valid;
  }
}
