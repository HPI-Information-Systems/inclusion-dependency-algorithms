package de.metanome.algorithms.mind;

import de.metanome.validation.ValidationStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class MindTest {

  @Mock
  private ValidationStrategy validationStrategy;

  private Mind mind;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.initMocks(this);
    mind = new Mind(validationStrategy);
  }

  @Test
  void runMind() {

  }
}
