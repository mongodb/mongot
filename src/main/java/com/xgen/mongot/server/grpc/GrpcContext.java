package com.xgen.mongot.server.grpc;

import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import io.grpc.Context;

public class GrpcContext {
  public static final Context.Key<SearchEnvoyMetadata> SEARCH_ENVOY_METADATA_KEY =
      Context.key("search-envoy-metadata");
  public static final Context.Key<Integer> ENVOY_ATTEMPT_COUNT_KEY =
      Context.key("envoy-attempt-count");
  public static final Context.Key<Integer> ENVOY_MAX_ATTEMPT_COUNT_KEY =
      Context.key("envoy-max-attempt-count");
  public static final Context.Key<Boolean> IS_SHADOW_KEY = Context.key("is-shadow");

  /**
   * Whether search envoy appears to be in front of this request, from the presence of the {@code
   * x-envoy-attempt-count} header. Distinct from {@link #ENVOY_ATTEMPT_COUNT_KEY}, which defaults
   * to 1 when the header is absent and so cannot tell a direct caller from a first envoy attempt.
   */
  public static final Context.Key<Boolean> ENVOY_IN_REQUEST_PATH_KEY =
      Context.key("envoy-in-request-path");
}
