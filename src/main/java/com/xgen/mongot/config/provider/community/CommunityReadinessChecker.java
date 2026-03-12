package com.xgen.mongot.config.provider.community;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalogservice.IndexStatsEntry;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.catalogservice.ServerStateEntry;
import com.xgen.mongot.config.manager.CachedIndexInfoProvider;
import com.xgen.mongot.config.manager.ConfigManager;
import com.xgen.mongot.index.IndexDetailedStatus;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.server.CommandServer;
import com.xgen.mongot.server.http.ReadinessChecker;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommunityReadinessChecker implements ReadinessChecker {

  private static final Logger LOG = LoggerFactory.getLogger(CommunityReadinessChecker.class);

  private final CommunityServerInfo serverInfo;
  private final ConfigManager configManager;
  private final CachedIndexInfoProvider indexInfoProvider;
  private final MetadataService metadataService;
  private final List<CommandServer> servers;

  // This can be called by unauthenticated connections to get the readiness state of the server.
  // To avoid overloading mongod if the readiness API gets excessive traffic we memorize the latest
  // results and cache the results for 10 seconds.
  private final Supplier<IndexesInAicSupplierOutput> indexesInAicSupplier =
      Suppliers.memoizeWithExpiration(this::getIndexesInAic, 10, TimeUnit.SECONDS);
  private final Supplier<IndexStatsSupplierOutput> indexEntriesPerIndexSupplier =
      Suppliers.memoizeWithExpiration(this::getIndexStatsPerIndex, 10, TimeUnit.SECONDS);

  @Var private volatile boolean isReady = false;

  public CommunityReadinessChecker(
      CommunityServerInfo serverInfo,
      ConfigManager configManager,
      CachedIndexInfoProvider indexInfoProvider,
      MetadataService metadataService,
      List<CommandServer> servers) {
    this.serverInfo = serverInfo;
    this.configManager = configManager;
    this.indexInfoProvider = indexInfoProvider;
    this.metadataService = metadataService;
    this.servers = servers;
  }

  @Override
  public boolean isReady(boolean allowFailedIndexes) throws Exception {

    // Once isReady is set to true, cache it in memory for the lifetime of the process to avoid
    // the readiness state from flapping as indexes are built/deleted causing the server being
    // removed from the LB.
    if (this.isReady) {
      return true;
    }

    if (this.servers.stream()
        .anyMatch(server -> server.getServerStatus() == CommandServer.ServerStatus.NOT_STARTED)) {
      LOG.info("Not ready, waiting on servers to be started...");
      return false;
    }

    if (!this.configManager.isReplicationInitialized()) {
      LOG.info("Not ready, waiting on replication to be initialized...");
      return false;
    }

    Optional<ServerStateEntry> serverStateEntry =
        this.metadataService.getServerState().get(this.serverInfo.id());

    // Treat as not ready if the server state entry is missing, the server is still marked
    // shutdown, or the readiness state has expired. CommunityMetadataUpdater will reset
    // these on its first run. Return not ready here so that run can complete before we
    // declare ready, avoiding races with the updater.
    if (serverStateEntry.isEmpty()
        || serverStateEntry.get().shutdown()
        || serverStateEntry.get().isReadinessStateExpired()) {
      LOG.info("Not ready, server state entry missing, shutdown, or readiness expired...");
      return false;
    }

    // If this server was previously marked as ready, skip the full readiness validation
    // (replication init, index builds, etc.) and immediately report as ready. We only require
    // indexes to be built before marking a *new* server as ready because routing queries to a new
    // server before indexes are built would cause failures and unavailability. For a restarting
    // server, the indexes were already built previously, so we don't want to wait for any
    // in-progress index builds before the k8s readiness probe passes and the server is added back
    // to the envoy LB, as that would unnecessarily reduce cluster capacity in the meantime.
    if (serverStateEntry.get().ready()) {
      LOG.info("Server was already marked as ready");
      this.isReady = true;
      return true;
    }

    this.indexInfoProvider.refreshIndexInfos();
    List<IndexInformation> indexInfos = this.indexInfoProvider.getIndexInfos();

    if (!hasAllIndexesFromCatalog(indexInfos)) {
      LOG.info("Not ready, mongot is missing indexes from catalog");
      return false;
    }

    if (indexInfos.isEmpty()) {
      LOG.info("No indexes to validate, server is ready!");
      return markReady();
    }

    if (!areIndexesReadyToQuery(indexInfos, allowFailedIndexes)) {
      LOG.info("Not ready, indexes not ready to serve queries...");
      return false;
    }

    LOG.info("Server is ready!");
    return markReady();
  }

  private boolean markReady() throws MetadataServiceException {
    this.metadataService
        .getServerState()
        .updateOne(this.serverInfo.id(), ServerStateEntry.updateReadinessStatus(true));
    this.isReady = true;
    return true;
  }

  private boolean hasAllIndexesFromCatalog(List<IndexInformation> indexInfos) throws Exception {
    IndexesInAicSupplierOutput indexesInCatalog = this.indexesInAicSupplier.get();
    if (indexesInCatalog.exception.isPresent()) {
      throw indexesInCatalog.exception.get();
    }

    return indexInfos.stream()
        .map(i -> i.getDefinition().getIndexId())
        .collect(Collectors.toSet())
        .containsAll(indexesInCatalog.indexIds);
  }

  private boolean areIndexesReadyToQuery(
      List<IndexInformation> indexInfos, boolean allowFailedIndexes) throws Exception {

    IndexStatsSupplierOutput indexStatsSupplierOutput = this.indexEntriesPerIndexSupplier.get();
    if (indexStatsSupplierOutput.exception.isPresent()) {
      throw indexStatsSupplierOutput.exception.get();
    }

    return indexInfos.stream()
        .allMatch(
            i ->
                isIndexInValidState(
                    i,
                    indexStatsSupplierOutput
                        .indexStatsEntries()
                        .getOrDefault(i.getDefinition().getIndexId(), Collections.emptyList()),
                    allowFailedIndexes));
  }

  private boolean isIndexInValidState(
      IndexInformation indexInfo,
      List<IndexStatsEntry> indexStatsEntries,
      boolean allowFailedIndexes) {
    Optional<IndexStatus> status = indexInfo.getMainIndex().map(IndexDetailedStatus::indexStatus);

    if (status.isEmpty()) {
      return false;
    }

    IndexStatus.StatusCode statusCode = status.get().getStatusCode();
    if (statusCode == IndexStatus.StatusCode.FAILED) {

      if (allowFailedIndexes) {
        LOG.info(
            "Bypassing failed index status check for index: {}",
            indexInfo.getDefinition().getIndexId());
        return true;
      }

      // Don't block readiness probe because of invalid index definitions
      if (isInvalidDefinitionFailure(status.get())
          && indexStatsEntries.stream()
              .map(IndexStatsEntry::mainIndex)
              .allMatch(i -> i.isEmpty() || isInvalidDefinitionFailure(i.get().status()))) {
        LOG.info(
            "Bypassing failed index status check for index '{}' "
                + "because index has invalid definition",
            indexInfo.getDefinition().getIndexId());
        return true;
      }

      return false;
    }

    return statusCode == IndexStatus.StatusCode.STEADY
        || statusCode == IndexStatus.StatusCode.RECOVERING_TRANSIENT
        || statusCode == IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT
        || statusCode == IndexStatus.StatusCode.DOES_NOT_EXIST
        || statusCode == IndexStatus.StatusCode.STALE;
  }

  private boolean isInvalidDefinitionFailure(IndexStatus status) {
    return status.getStatusCode() == IndexStatus.StatusCode.FAILED
        && status.getMessage().isPresent()
        && status.getMessage().get().startsWith("Invalid definition: ");
  }

  private IndexesInAicSupplierOutput getIndexesInAic() {
    Set<ObjectId> indexesInAic;
    try {
      indexesInAic =
          this.metadataService.getAuthoritativeIndexCatalog().listIndexDefinitions().stream()
              .map(IndexDefinition::getIndexId)
              .collect(Collectors.toUnmodifiableSet());
    } catch (Exception e) {
      LOG.warn("Exception fetching indexes from AIC in readiness probe", e);
      // The memoized supplier will not memorize exceptions and subsequent calls will recall the
      // index stats collection. To avoid overloading the DB we cache the exception within the
      // supplier as a POJO.
      return IndexesInAicSupplierOutput.error(e);
    }

    return IndexesInAicSupplierOutput.success(indexesInAic);
  }

  private IndexStatsSupplierOutput getIndexStatsPerIndex() {
    Map<ObjectId, List<IndexStatsEntry>> indexStatsEntries;
    try {
      indexStatsEntries =
          this.metadataService.getIndexStats().list().stream()
              .collect(Collectors.groupingBy(i -> i.key().indexId()));
    } catch (MetadataServiceException e) {
      LOG.warn("Metadata service exception fetching index stats from the probe", e);
      // The memoized supplier will not memorize exceptions and subsequent calls will recall the
      // index stats collection. To avoid overloading the DB we cache the exception within the
      // supplier as a POJO.
      return IndexStatsSupplierOutput.error(e);
    }

    return IndexStatsSupplierOutput.success(indexStatsEntries);
  }

  private record IndexStatsSupplierOutput(
      Map<ObjectId, List<IndexStatsEntry>> indexStatsEntries, Optional<Exception> exception) {

    IndexStatsSupplierOutput {
      Objects.requireNonNull(indexStatsEntries);
      Objects.requireNonNull(exception);
    }

    static IndexStatsSupplierOutput error(Exception e) {
      return new IndexStatsSupplierOutput(Collections.emptyMap(), Optional.of(e));
    }

    static IndexStatsSupplierOutput success(
        Map<ObjectId, List<IndexStatsEntry>> indexStatsEntries) {
      return new IndexStatsSupplierOutput(indexStatsEntries, Optional.empty());
    }
  }

  private record IndexesInAicSupplierOutput(Set<ObjectId> indexIds, Optional<Exception> exception) {

    IndexesInAicSupplierOutput {
      Objects.requireNonNull(indexIds);
      Objects.requireNonNull(exception);
    }

    static IndexesInAicSupplierOutput error(Exception e) {
      return new IndexesInAicSupplierOutput(Collections.emptySet(), Optional.of(e));
    }

    static IndexesInAicSupplierOutput success(Set<ObjectId> indexIds) {
      return new IndexesInAicSupplierOutput(indexIds, Optional.empty());
    }
  }
}
