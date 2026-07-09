package com.xgen.mongot.replication.mongodb.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.truth.Truth;
import com.mongodb.MongoCommandException;
import com.xgen.mongot.util.mongodb.Errors;
import java.util.Optional;
import org.junit.Test;

public class QueryRejectedBySettingsExceptionUtilsTest {

  @Test
  public void testIsQueryRejectedBySettingsErrorReturnsTrueForCode411() {
    var exception = mock(MongoCommandException.class);
    when(exception.getCode()).thenReturn(Errors.QUERY_REJECTED_BY_SETTINGS.code);

    assertTrue(
        QueryRejectedBySettingsExceptionUtils.isQueryRejectedBySettingsError(
            Optional.of(exception)));
  }

  @Test
  public void testIsQueryRejectedBySettingsErrorReturnsFalseForOtherCode() {
    var exception = mock(MongoCommandException.class);
    when(exception.getCode()).thenReturn(Errors.NAMESPACE_NOT_FOUND.code);

    assertFalse(
        QueryRejectedBySettingsExceptionUtils.isQueryRejectedBySettingsError(
            Optional.of(exception)));
  }

  @Test
  public void testIsQueryRejectedBySettingsErrorReturnsFalseForEmptyCause() {
    assertFalse(
        QueryRejectedBySettingsExceptionUtils.isQueryRejectedBySettingsError(Optional.empty()));
  }

  @Test
  public void testIsQueryRejectedBySettingsErrorReturnsFalseForNonMongoException() {
    assertFalse(
        QueryRejectedBySettingsExceptionUtils.isQueryRejectedBySettingsError(
            Optional.of(new RuntimeException("boom"))));
  }

  @Test
  public void testGetQueryRejectedBySettingsErrorMessageContainsDriverMessageAndGuidance() {
    var exception = mock(MongoCommandException.class);
    when(exception.getErrorMessage()).thenReturn("Query rejected by admin settings");

    var result =
        QueryRejectedBySettingsExceptionUtils.getQueryRejectedBySettingsErrorMessage(exception);

    Truth.assertThat(result).contains("Query rejected by admin settings");
    Truth.assertThat(result).contains("removeQuerySettings");
  }

  @Test
  public void testGetQueryRejectedBySettingsErrorMessageWhenDriverErrorIsNull() {
    var exception = mock(MongoCommandException.class);
    when(exception.getErrorMessage()).thenReturn(null);

    var result =
        QueryRejectedBySettingsExceptionUtils.getQueryRejectedBySettingsErrorMessage(exception);

    Truth.assertThat(result).contains("unknown error");
  }

  @Test
  public void testGetQueryRejectedBySettingsErrorMessageWhenDriverErrorIsBlank() {
    var exception = mock(MongoCommandException.class);
    when(exception.getErrorMessage()).thenReturn("   ");

    var result =
        QueryRejectedBySettingsExceptionUtils.getQueryRejectedBySettingsErrorMessage(exception);

    Truth.assertThat(result).contains("unknown error");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetQueryRejectedBySettingsErrorMessageNullIsValidated() {
    QueryRejectedBySettingsExceptionUtils.getQueryRejectedBySettingsErrorMessage(null);
  }
}
