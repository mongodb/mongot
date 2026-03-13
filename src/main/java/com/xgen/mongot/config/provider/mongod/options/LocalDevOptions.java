package com.xgen.mongot.config.provider.mongod.options;

import com.google.common.net.HostAndPort;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.Optional;

public class LocalDevOptions {
  public final HostAndPort mongodHost;

  public final Optional<HostAndPort> mongosHost;
  public final int mongotPort;
  public final Optional<SocketAddress> grpcAddress;
  public final Path mongotDataDir;
  public final Path keyFilePath;
  public final boolean verboseListResponse;
  public final boolean metricsEnable;
  public final Optional<String> metricsAddress;
  public final Optional<String> metricsCommonLabels;
  public final Optional<String> metricsDenyLabels;
  public final Optional<String> voyageApiKey;
  public final Optional<Integer> mvWriteRateLimitRps;

  public LocalDevOptions(
      HostAndPort mongodHost,
      Optional<HostAndPort> mongosHost,
      int mongotPort,
      Optional<SocketAddress> grpcAddress,
      Path mongotDataDir,
      Path keyFilePath,
      boolean verboseListResponse,
      boolean metricsEnable,
      Optional<String> metricsAddress,
      Optional<String> metricsCommonLabels,
      Optional<String> metricsDenyLabels,
      Optional<String> voyageApiKey,
      Optional<Integer> mvWriteRateLimitRps) {
    this.mongodHost = mongodHost;
    this.mongosHost = mongosHost;
    this.mongotPort = mongotPort;
    this.grpcAddress = grpcAddress;
    this.mongotDataDir = mongotDataDir;
    this.keyFilePath = keyFilePath;
    this.verboseListResponse = verboseListResponse;
    this.metricsEnable = metricsEnable;
    this.metricsAddress = metricsAddress;
    this.metricsCommonLabels = metricsCommonLabels;
    this.metricsDenyLabels = metricsDenyLabels;
    this.voyageApiKey = voyageApiKey;
    this.mvWriteRateLimitRps = mvWriteRateLimitRps;
  }

  @Override
  public String toString() {
    return "LocalDevOptions{"
        + "mongodHost="
        + this.mongodHost
        + ", mongosHost="
        + this.mongosHost
        + ", mongotPort="
        + this.mongotPort
        + ", grpcAddress="
        + this.grpcAddress
        + ", mongotDataDir="
        + this.mongotDataDir
        + ", keyFilePath="
        + this.keyFilePath
        + ", verboseListResponse="
        + this.verboseListResponse
        + ", metricsEnable="
        + this.metricsEnable
        + ", metricsAddress="
        + this.metricsAddress
        + ", metricsCommonLabels="
        + this.metricsCommonLabels
        + ", metricsDenyLabels="
        + this.metricsDenyLabels
        + ", mvWriteRateLimitRps="
        + this.mvWriteRateLimitRps
        + '}';
  }
}
