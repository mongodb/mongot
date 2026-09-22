package com.xgen.mongot.server.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.Optional;

public class SearchEnvoyMetadataInterceptor implements ServerInterceptor {

  /**
   * Attempt budget assumed when search envoy does not advertise one, either because the request did
   * not come through search envoy or because it came from a version of mms that predates the {@code
   * x-envoy-max-attempt-count} header. Matches the budget mms configures by default.
   */
  static final int DEFAULT_MAX_ATTEMPT_COUNT = 3;

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    Optional<SearchEnvoyMetadata> searchEnvoyMetadata =
        Optional.ofNullable(headers.get(GrpcMetadata.SEARCH_ENVOY_METADATA_KEY));

    int attemptCount = parseAttemptCount(headers);
    int maxAttemptCount = parseMaxAttemptCount(headers);
    boolean isShadow = isShadowAuthority(call.getAuthority());

    Context context =
        Context.current()
            .withValue(
                GrpcContext.SEARCH_ENVOY_METADATA_KEY,
                searchEnvoyMetadata.orElse(SearchEnvoyMetadata.getDefaultInstance()))
            .withValue(GrpcContext.ENVOY_ATTEMPT_COUNT_KEY, attemptCount)
            .withValue(GrpcContext.ENVOY_MAX_ATTEMPT_COUNT_KEY, maxAttemptCount)
            .withValue(GrpcContext.IS_SHADOW_KEY, isShadow);

    return Contexts.interceptCall(context, call, headers, next);
  }

  /**
   * A warm-up traffic shadow is identified by the "-shadow" suffix envoy's shadow writer appends to
   * the host portion of the request :authority when mirroring (search-envoy
   * shadow_writer_impl.cc#shadowAppendedHost). The suffix goes before any ":port", so strip a
   * trailing numeric port before testing.
   */
  @VisibleForTesting
  static boolean isShadowAuthority(String authority) {
    if (authority == null) {
      return false;
    }
    int lastColon = authority.lastIndexOf(':');
    boolean hasNumericPort =
        lastColon > 0
            && lastColon < authority.length() - 1
            && authority.substring(lastColon + 1).chars().allMatch(Character::isDigit);
    String host = hasNumericPort ? authority.substring(0, lastColon) : authority;
    return host.endsWith("-shadow");
  }

  private static int parseAttemptCount(Metadata headers) {
    // Envoy sets `x-envoy-attempt-count` starting at 1 on the initial request and increments it
    // on each retry (see envoy's `include_request_attempt_count` route option). When the header
    // is missing (direct gRPC clients, or envoy's `include_request_attempt_count` not enabled)
    // or malformed, we fall back to 1 — i.e. "treat this as the first attempt". This is the
    // conservative default: it matches envoy's own first-attempt value, and downstream retry
    // logic in `ServerCallHandler#shouldRetryViaEnvoy` will still permit signalling a retry
    // (1 < the attempt budget), so we don't silently disable retries for clients that omit the
    // header.
    String value = headers.get(GrpcMetadata.ENVOY_ATTEMPT_COUNT_KEY);
    if (value == null) {
      return 1;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return 1;
    }
  }

  private static int parseMaxAttemptCount(Metadata headers) {
    // mongot has no way to prove a header came from search envoy rather than from whoever opened
    // the connection, which is why the budget only bounds how long mongot keeps asking envoy to
    // retry — it never decides how many requests exist. That is envoy's `num_retries`, and requests
    // that do traverse search envoy arrive with this header overwritten or stripped by the route
    // config. So an inflated value from a caller that bypassed envoy costs that caller a plain gRPC
    // RESOURCE_EXHAUSTED on its own query instead of a labelled error, and nothing beyond it.
    String value = headers.get(GrpcMetadata.ENVOY_MAX_ATTEMPT_COUNT_KEY);
    if (value == null) {
      return DEFAULT_MAX_ATTEMPT_COUNT;
    }
    int parsed;
    try {
      parsed = Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return DEFAULT_MAX_ATTEMPT_COUNT;
    }
    if (parsed < 1) {
      return DEFAULT_MAX_ATTEMPT_COUNT;
    }
    return parsed;
  }
}
