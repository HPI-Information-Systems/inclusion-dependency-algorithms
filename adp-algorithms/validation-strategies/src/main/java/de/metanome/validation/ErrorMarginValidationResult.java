package de.metanome.validation;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class ErrorMarginValidationResult implements ValidationResult {

  private final boolean valid;
  private final double errorMargin;

  @Override
  public boolean isValid() {
    return valid;
  }

  public double errorMargin() {
    return errorMargin;
  }
}
