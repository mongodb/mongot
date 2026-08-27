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
}
