package com.xgen.mongot.server.grpc;

import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import io.grpc.Metadata;
import io.grpc.protobuf.ProtoUtils;
import java.util.UUID;

public class GrpcMetadata {
  private static final Metadata.AsciiMarshaller<Integer> INT32_MARSHALLER =
      new Metadata.AsciiMarshaller<>() {

        @Override
        public String toAsciiString(Integer value) {
          return value.toString();
        }

        @Override
        public Integer parseAsciiString(String serialized) {
          return Integer.parseInt(serialized);
        }
      };

  private static final Metadata.AsciiMarshaller<UUID> UUID_MARSHALLER =
      new Metadata.AsciiMarshaller<>() {

        @Override
        public String toAsciiString(UUID value) {
          return value.toString();
        }

        @Override
        public UUID parseAsciiString(String serialized) {
          return UUID.fromString(serialized);
        }
      };

  // Request headers populated by search envoy.
  public static final Metadata.Key<SearchEnvoyMetadata> SEARCH_ENVOY_METADATA_KEY =
      Metadata.Key.of(
          "search-envoy-metadata-bin",
          ProtoUtils.metadataMarshaller(SearchEnvoyMetadata.getDefaultInstance()));

  // Request headers for MongoDB gRPC protocol.
  public static final Metadata.Key<Integer> MONGODB_WIRE_VERSION_METADATA_KEY =
      Metadata.Key.of("mongodb-wireversion", INT32_MARSHALLER);
  public static final Metadata.Key<UUID> MONGODB_CLIENT_ID_METADATA_KEY =
      Metadata.Key.of("mongodb-clientid", UUID_MARSHALLER);
  public static final Metadata.Key<String> MONGODB_CLIENT_METADATA_KEY =
      Metadata.Key.of("mongodb-client", Metadata.ASCII_STRING_MARSHALLER);
  public static final Metadata.Key<String> ENVOY_ATTEMPT_COUNT_KEY =
      Metadata.Key.of("x-envoy-attempt-count", Metadata.ASCII_STRING_MARSHALLER);
  // Not an envoy built-in: mms adds this to the mongot routes it generates, carrying the attempt
  // budget of the retry policy that produced `x-envoy-attempt-count`.
  public static final Metadata.Key<String> ENVOY_MAX_ATTEMPT_COUNT_KEY =
      Metadata.Key.of("x-envoy-max-attempt-count", Metadata.ASCII_STRING_MARSHALLER);

  // Response headers for MongoDB gRPC protocol.
  public static final Metadata.Key<Integer> MONGODB_MAX_WIRE_VERSION_METADATA_KEY =
      Metadata.Key.of("mongodb-maxwireversion", INT32_MARSHALLER);
}
