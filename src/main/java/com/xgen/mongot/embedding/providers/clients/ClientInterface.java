package com.xgen.mongot.embedding.providers.clients;

import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.congestion.DynamicSemaphore;
import java.util.List;

// TODO(CLOUDP-296846): Investigate whether we want to support multiple embedding types other than
// floats/double
public interface ClientInterface {
  List<VectorOrError> embed(List<String> inputs, EmbeddingRequestContext context)
      throws EmbeddingProviderTransientException, EmbeddingProviderNonTransientException;

  void updateConfig(EmbeddingModelConfig.ConsolidatedWorkloadParams serviceParams);

  /** Sets an optional congestion control semaphore. Only VoyageClient overrides this. */
  default void setCongestionSemaphore(DynamicSemaphore semaphore) {}
}
