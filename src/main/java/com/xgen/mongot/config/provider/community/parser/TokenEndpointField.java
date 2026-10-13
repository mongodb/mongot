package com.xgen.mongot.config.provider.community.parser;

import com.xgen.mongot.util.bson.parser.ClassField;
import com.xgen.mongot.util.bson.parser.ValueEncoder;
import java.net.URI;
import java.net.URISyntaxException;
import org.bson.BsonString;

public final class TokenEndpointField {
  public static final ValueEncoder<URI> ENCODER = uri -> new BsonString(uri.toString());
  public static final ClassField.FromValueParser<URI> PARSER =
      (context, value) -> {
        if (!value.isString()) {
          context.handleUnexpectedType("string", value.getBsonType());
        }
        URI uri;
        try {
          uri = new URI(value.asString().getValue());
        } catch (URISyntaxException e) {
          return context.handleSemanticError("is not a valid URI: " + e.getReason());
        }
        boolean isHttps = "https".equalsIgnoreCase(uri.getScheme());
        boolean hasHost = uri.getHost() != null && !uri.getHost().isEmpty();
        if (!isHttps || !hasHost || uri.getUserInfo() != null || uri.getFragment() != null) {
          return context.handleSemanticError(
              "must be an absolute https URI with a host, without userinfo or a fragment");
        }
        // getPort() == -1 means no port, which is valid; a specified port is range-checked like
        // every other port-bearing config field (mirrors the driver's ConnectionString).
        int port = uri.getPort();
        if (port != -1 && (port < 1 || port > 65535)) {
          return context.handleSemanticError("must specify a port between 1 and 65535");
        }
        return uri;
      };
}
