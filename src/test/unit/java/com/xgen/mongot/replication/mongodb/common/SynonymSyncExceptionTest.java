package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.regex.RegexInterruptedException;
import com.xgen.mongot.util.regex.RegexLimitExceededException;
import java.util.Optional;
import org.bson.BsonTimestamp;
import org.junit.Test;

public class SynonymSyncExceptionTest {
  @Test
  public void wrapIfThrows_regexLimit_failsWithoutRescanning() {
    var failure = new RegexLimitExceededException();

    var translated =
        assertThrows(
            SynonymSyncException.class,
            () ->
                SynonymSyncException.wrapIfThrows(
                    () -> {
                      throw failure;
                    }));

    assertThat(translated.getType()).isEqualTo(SynonymSyncException.Type.FAILED);
    assertThat(translated.getMessage()).isEqualTo(failure.getMessage());
    assertThat(translated.getCause()).isSameInstanceAs(failure);
    assertThat(
            SynonymSyncException.withOpTime(translated, Optional.of(new BsonTimestamp(1, 1)))
                .getCause())
        .isSameInstanceAs(failure);
  }

  @Test
  public void wrapIfThrows_regexInterrupted_isShutdown() {
    var translated =
        assertThrows(
            SynonymSyncException.class,
            () ->
                SynonymSyncException.wrapIfThrows(
                    () -> {
                      throw new RegexInterruptedException();
                    }));

    assertThat(translated.isShutDown()).isTrue();
  }
}
