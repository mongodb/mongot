package com.xgen.mongot.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

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
}
