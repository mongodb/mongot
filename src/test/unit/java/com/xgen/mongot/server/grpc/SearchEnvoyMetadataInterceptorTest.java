package com.xgen.mongot.server.grpc;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import org.junit.Test;

public class SearchEnvoyMetadataInterceptorTest {
  @Test
  public void testPopulateSearchEnvoyMetadata() {
    var searchEnvoyMetadata =
        SearchEnvoyMetadata.newBuilder().setRoutedFromAnotherShard(true).build();

    Metadata metadata = new Metadata();
    metadata.put(GrpcMetadata.SEARCH_ENVOY_METADATA_KEY, searchEnvoyMetadata);
    triggerInterceptor(metadata, searchEnvoyMetadata);
  }

  @Test
  public void testPopulateSearchEnvoyMetadata_defaultInstance() {
    Metadata metadata = new Metadata();
    metadata.put(GrpcMetadata.SEARCH_ENVOY_METADATA_KEY, SearchEnvoyMetadata.getDefaultInstance());
    triggerInterceptor(metadata, SearchEnvoyMetadata.getDefaultInstance());
  }

  @Test
  public void testPopulateSearchEnvoyMetadata_headerDoesNotExist() {
    Metadata metadata = new Metadata();
    triggerInterceptor(metadata, SearchEnvoyMetadata.getDefaultInstance());
  }

  @Test
  public void testAttemptCount_parsedFromHeader() {
    Metadata metadata = new Metadata();
    metadata.put(GrpcMetadata.ENVOY_ATTEMPT_COUNT_KEY, "2");
    triggerInterceptorAndCheckAttemptCount(metadata, 2);
  }

  @Test
  public void testAttemptCount_defaultsTo1WhenMissing() {
    Metadata metadata = new Metadata();
    triggerInterceptorAndCheckAttemptCount(metadata, 1);
  }

  @Test
  public void testAttemptCount_defaultsTo1WhenInvalid() {
    Metadata metadata = new Metadata();
    metadata.put(GrpcMetadata.ENVOY_ATTEMPT_COUNT_KEY, "not-a-number");
    triggerInterceptorAndCheckAttemptCount(metadata, 1);
  }

  @Test
  public void testAttemptCount_parsesLargeValue() {
    Metadata metadata = new Metadata();
    metadata.put(GrpcMetadata.ENVOY_ATTEMPT_COUNT_KEY, "5");
    triggerInterceptorAndCheckAttemptCount(metadata, 5);
  }

  @Test
  public void testMaxAttemptCount_parsedFromHeader() {
    Metadata metadata = new Metadata();
    metadata.put(GrpcMetadata.ENVOY_MAX_ATTEMPT_COUNT_KEY, "4");
    triggerInterceptorAndCheckMaxAttemptCount(metadata, 4);
  }

  @Test
  public void testMaxAttemptCount_defaultsWhenMissing() {
    Metadata metadata = new Metadata();
    triggerInterceptorAndCheckMaxAttemptCount(
        metadata, SearchEnvoyMetadataInterceptor.DEFAULT_MAX_ATTEMPT_COUNT);
  }

  @Test
  public void testMaxAttemptCount_defaultsWhenInvalid() {
    Metadata metadata = new Metadata();
    metadata.put(GrpcMetadata.ENVOY_MAX_ATTEMPT_COUNT_KEY, "not-a-number");
    triggerInterceptorAndCheckMaxAttemptCount(
        metadata, SearchEnvoyMetadataInterceptor.DEFAULT_MAX_ATTEMPT_COUNT);
  }

  @Test
  public void testMaxAttemptCount_defaultsWhenBelowOne() {
    Metadata metadata = new Metadata();
    metadata.put(GrpcMetadata.ENVOY_MAX_ATTEMPT_COUNT_KEY, "0");
    triggerInterceptorAndCheckMaxAttemptCount(
        metadata, SearchEnvoyMetadataInterceptor.DEFAULT_MAX_ATTEMPT_COUNT);
  }

  @Test
  public void isShadowAuthority_nullAuthority_returnsFalse() {
    assertThat(SearchEnvoyMetadataInterceptor.isShadowAuthority(null)).isFalse();
  }

  @Test
  public void isShadowAuthority_plainHost_returnsFalse() {
    assertThat(SearchEnvoyMetadataInterceptor.isShadowAuthority("myhost")).isFalse();
  }

  @Test
  public void isShadowAuthority_shadowHost_returnsTrue() {
    assertThat(SearchEnvoyMetadataInterceptor.isShadowAuthority("myhost-shadow")).isTrue();
  }

  @Test
  public void isShadowAuthority_shadowHostWithPort_returnsTrue() {
    assertThat(SearchEnvoyMetadataInterceptor.isShadowAuthority("myhost-shadow:8080")).isTrue();
  }

  @Test
  public void isShadowAuthority_plainHostWithPort_returnsFalse() {
    assertThat(SearchEnvoyMetadataInterceptor.isShadowAuthority("myhost:8080")).isFalse();
  }

  @Test
  public void isShadowAuthority_shadowSubstringNotSuffix_returnsFalse() {
    assertThat(SearchEnvoyMetadataInterceptor.isShadowAuthority("shadow-myhost")).isFalse();
  }

  @Test
  public void testIsShadow_trueWhenAuthorityHasShadowSuffix() {
    triggerInterceptorAndCheckShadow("myhost-shadow:8080", true);
  }

  @Test
  public void testIsShadow_falseForPlainAuthority() {
    triggerInterceptorAndCheckShadow("myhost:8080", false);
  }

  @Test
  public void testIsShadow_falseWhenAuthorityMissing() {
    triggerInterceptorAndCheckShadow(null, false);
  }

  @SuppressWarnings("unchecked")
  private void triggerInterceptorAndCheckShadow(String authority, boolean expectedShadow) {
    Metadata metadata = new Metadata();
    ServerCall<Object, Object> serverCall = mock(ServerCall.class);
    when(serverCall.getAuthority()).thenReturn(authority);
    ServerCall.Listener<Object> listener = mock(ServerCall.Listener.class);
    doAnswer(
            invocation -> {
              assertEquals(expectedShadow, GrpcContext.IS_SHADOW_KEY.get());
              return null;
            })
        .when(listener)
        .onReady();
    var interceptor = new SearchEnvoyMetadataInterceptor();
    var interceptedCall =
        interceptor.interceptCall(serverCall, metadata, (call, headers) -> listener);
    interceptedCall.onReady();
  }

  @Test
  public void testEnvoyInRequestPath_trueWhenAttemptCountHeaderPresent() {
    Metadata metadata = new Metadata();
    metadata.put(GrpcMetadata.ENVOY_ATTEMPT_COUNT_KEY, "1");
    triggerInterceptorAndCheckEnvoyInRequestPath(metadata, true);
  }

  @Test
  public void testEnvoyInRequestPath_falseWhenAttemptCountHeaderMissing() {
    Metadata metadata = new Metadata();
    triggerInterceptorAndCheckEnvoyInRequestPath(metadata, false);
  }

  /**
   * A malformed header still came from something that sets it, so the request is treated as
   * envoy-fronted even though the count itself falls back to 1.
   */
  @Test
  public void testEnvoyInRequestPath_trueWhenAttemptCountHeaderMalformed() {
    Metadata metadata = new Metadata();
    metadata.put(GrpcMetadata.ENVOY_ATTEMPT_COUNT_KEY, "not-a-number");
    triggerInterceptorAndCheckEnvoyInRequestPath(metadata, true);
  }

  /** The max-attempt-count header alone is not evidence of envoy; only the attempt count is. */
  @Test
  public void testEnvoyInRequestPath_falseWhenOnlyMaxAttemptCountPresent() {
    Metadata metadata = new Metadata();
    metadata.put(GrpcMetadata.ENVOY_MAX_ATTEMPT_COUNT_KEY, "4");
    triggerInterceptorAndCheckEnvoyInRequestPath(metadata, false);
  }

  @SuppressWarnings("unchecked")
  private void triggerInterceptor(
      Metadata metadata, SearchEnvoyMetadata expectedSearchEnvoyMetadata) {
    ServerCall<Object, Object> serverCall = mock(ServerCall.class);
    ServerCall.Listener<Object> listener = mock(ServerCall.Listener.class);
    doAnswer(
            invocation -> {
              assertEquals(
                  expectedSearchEnvoyMetadata, GrpcContext.SEARCH_ENVOY_METADATA_KEY.get());
              return null;
            })
        .when(listener)
        .onReady();
    var interceptor = new SearchEnvoyMetadataInterceptor();
    var interceptedCall =
        interceptor.interceptCall(serverCall, metadata, (call, headers) -> listener);
    interceptedCall.onReady();
  }

  @SuppressWarnings("unchecked")
  private void triggerInterceptorAndCheckAttemptCount(Metadata metadata, int expectedAttemptCount) {
    ServerCall<Object, Object> serverCall = mock(ServerCall.class);
    ServerCall.Listener<Object> listener = mock(ServerCall.Listener.class);
    doAnswer(
            invocation -> {
              assertEquals(
                  Integer.valueOf(expectedAttemptCount), GrpcContext.ENVOY_ATTEMPT_COUNT_KEY.get());
              return null;
            })
        .when(listener)
        .onReady();
    var interceptor = new SearchEnvoyMetadataInterceptor();
    var interceptedCall =
        interceptor.interceptCall(serverCall, metadata, (call, headers) -> listener);
    interceptedCall.onReady();
  }

  @SuppressWarnings("unchecked")
  private void triggerInterceptorAndCheckMaxAttemptCount(
      Metadata metadata, int expectedMaxAttemptCount) {
    ServerCall<Object, Object> serverCall = mock(ServerCall.class);
    ServerCall.Listener<Object> listener = mock(ServerCall.Listener.class);
    doAnswer(
            invocation -> {
              assertEquals(
                  Integer.valueOf(expectedMaxAttemptCount),
                  GrpcContext.ENVOY_MAX_ATTEMPT_COUNT_KEY.get());
              return null;
            })
        .when(listener)
        .onReady();
    var interceptor = new SearchEnvoyMetadataInterceptor();
    var interceptedCall =
        interceptor.interceptCall(serverCall, metadata, (call, headers) -> listener);
    interceptedCall.onReady();
  }

  @SuppressWarnings("unchecked")
  private void triggerInterceptorAndCheckEnvoyInRequestPath(
      Metadata metadata, boolean expectedEnvoyInRequestPath) {
    ServerCall<Object, Object> serverCall = mock(ServerCall.class);
    ServerCall.Listener<Object> listener = mock(ServerCall.Listener.class);
    doAnswer(
            invocation -> {
              assertEquals(
                  Boolean.valueOf(expectedEnvoyInRequestPath),
                  GrpcContext.ENVOY_IN_REQUEST_PATH_KEY.get());
              return null;
            })
        .when(listener)
        .onReady();
    var interceptor = new SearchEnvoyMetadataInterceptor();
    var interceptedCall =
        interceptor.interceptCall(serverCall, metadata, (call, headers) -> listener);
    interceptedCall.onReady();
  }
}
