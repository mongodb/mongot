package com.xgen.mongot.index.definition;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.base.CaseFormat;
import java.util.List;
import org.junit.Test;

public class ModalityTest {

  @Test
  public void fromValue_lowercaseWireValue_returnsConstant() {
    assertThat(Modality.fromValue("text")).isEqualTo(Modality.TEXT);
    assertThat(Modality.TEXT.value()).isEqualTo("text");
  }

  @Test
  public void value_matchesSpellingAcceptedByParser() {
    for (Modality modality : Modality.values()) {
      String parserSpelling =
          CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, modality.name());

      assertThat(modality.value()).isEqualTo(parserSpelling);
      assertThat(Modality.fromValue(parserSpelling)).isEqualTo(modality);
    }
  }

  @Test
  public void fromValue_anyUppercaseOrMixedCaseSpelling_throws() {
    for (String value : List.of("TEXT", "Text", "tEXt", "text ")) {
      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> Modality.fromValue(value));

      assertThat(exception).hasMessageThat().contains("modality '" + value + "' must be one of");
    }
  }

  @Test
  public void fromValue_unsupportedModality_throws() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> Modality.fromValue("image"));

    assertThat(exception).hasMessageThat().contains("must be one of [text]");
  }
}
