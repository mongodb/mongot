package com.xgen.mongot.community;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.config.provider.community.CommunityConfig;
import com.xgen.mongot.config.provider.community.OnPremMongotBootstrapper;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.lucene.vector.Similarity;
import com.xgen.mongot.logging.Logging;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.MongotVersionResolver;
import com.xgen.mongot.util.security.Security;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.IVersionProvider;

/** Entrypoint for the mongot community server. */
@CommandLine.Command(
    name = "mongot_community",
    mixinStandardHelpOptions = true,
    versionProvider = MongotCommunity.class)
public class MongotCommunity implements Runnable, IVersionProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MongotCommunity.class);

  @CommandLine.Option(names = "--config", description = "Path to the mongot YAML config file")
  public Path configPath;

  // This flag informs the system it was started up in test mode and modifies some behaviour when
  // running $listSearchIndexes for internal e2e and integration tests. It should not be applied
  // when running in prod.
  //
  // This flag does 3 things:
  // 1. Includes numDocs in the $listSearchIndexes response as it's needed for internal e2e tests.
  // 2. When calling $listSearchIndexes on the indexCatalog collection returns ALL indexes.
  // 3. Increases the frequency the CommunityMetadataUpdater runs to facilitate more timely status
  //    updates when calling $listSearchIndexes
  @CommandLine.Option(
      names = "--internalListAllIndexesForTesting",
      hidden = true,
      description =
          "Configure Community to return extended $listSearchIndexes response data,"
              + " used for internal e2e testing",
      defaultValue = "false")
  public boolean internalListAllIndexesForTesting;

  // CLI options that set fields of the mongot YAML config file. These do not override the config
  // file: if a field is provided both in the YAML config and via the CLI, an error is thrown.
  @CommandLine.Mixin
  public CommunityConfig.CommunityConfigCliOptionRegistry communityConfigCliOptionRegistry =
      new CommunityConfig.CommunityConfigCliOptionRegistry();

  @Override
  public void run() {
    LOG.atInfo().addKeyValue("mongotVersion", getVersion()[0]).log("[Starting Mongot]");

    // Enable translation of java.util.logging logs from third part libraries to SLF4J.
    Logging.bridgeJulToSlf4j();

    // Set the default uncaught exception handler, so if any thread terminates
    // due to an uncaught exception, mongot will fail.
    Crash.setDefaultUncaughtExceptionHandler();

    // install security providers used by mongot's modules
    Security.installFipsSecurityProvider();

    // Load native dependencies of the vector similarity library.
    Similarity.load();

    // Verify Java version
    if (Runtime.version().feature() != 21) {
      Crash.because("should be running Java 21").now();
    }

    // On-prem editions don't use DFF; communityDefault() seeds the flags that should default on
    // for on-prem. This instance is created only once per process.
    var dynamicFeatureFlagRegistry = DynamicFeatureFlagRegistry.communityDefault();
    var featureFlags = FeatureFlags.communityDefaults();

    Crash.because("failed to bootstrap")
        .ifThrows(
            () ->
                new OnPremMongotBootstrapper(
                        dynamicFeatureFlagRegistry, featureFlags, DeploymentEnvironment.COMMUNITY)
                    .bootstrap(
                        this.configPath,
                        this.internalListAllIndexesForTesting,
                        this.communityConfigCliOptionRegistry.traverse()));
  }

  public static void main(String[] args) {
    new CommandLine(new MongotCommunity())
        .registerConverter(HostAndPort.class, HostAndPort::fromString)
        .execute(args);
  }

  @Override
  public String[] getVersion() {
    return new String[] {MongotVersionResolver.create().getVersion()};
  }
}
