package com.xgen.testing.mongot.embedding.providers;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderBatchingException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.providers.clients.ClientInterface;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.util.bson.Vector;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/** Fake Embedding Provider client used for tests. It generates vectors from input hashes. */
public class FakeEmbeddingProviderClient implements ClientInterface {
  private static final HashFunction HASHER = Hashing.sha256();
  // if input string is found in errorInputSet, will return error message in VectorOrError.
  private final Set<String> localErrorInputSet;
  private final Set<String> transientErrorInputSet;
  private final Set<String> nonTransientErrorInputSet;
  private final int maxBatchSizeCharLimit;
  private final List<EmbeddingRequestContext> receivedContexts = new CopyOnWriteArrayList<>();

  public FakeEmbeddingProviderClient(
      Set<String> localErrorInputSet,
      Set<String> transientErrorInputSet,
      Set<String> nonTransientErrorInputSet,
      int maxBatchSizeCharLimit) {
    this.localErrorInputSet = localErrorInputSet;
    this.transientErrorInputSet = transientErrorInputSet;
    this.nonTransientErrorInputSet = nonTransientErrorInputSet;
    this.maxBatchSizeCharLimit = maxBatchSizeCharLimit;
  }

  /** Returns the context passed to every {@link #embed} call so far, in call order. */
  public List<EmbeddingRequestContext> getReceivedContexts() {
    return List.copyOf(this.receivedContexts);
  }

  @Override
  public List<VectorOrError> embed(List<String> input, EmbeddingRequestContext context)
      throws EmbeddingProviderTransientException, EmbeddingProviderNonTransientException {
    this.receivedContexts.add(context);
    if (input.stream().mapToInt(String::length).sum() > this.maxBatchSizeCharLimit) {
      throw new EmbeddingProviderBatchingException(
          "Please lower the number of tokens in the batch.");
    }
    return input.stream()
        .map(
            value -> {
              if (this.localErrorInputSet.contains(value)) {
                return new VectorOrError(value);
              } else if (this.transientErrorInputSet.contains(value)) {
                throw new EmbeddingProviderTransientException("Transient error");
              } else if (this.nonTransientErrorInputSet.contains(value)) {
                throw new EmbeddingProviderNonTransientException("Non transient error");
              } else {
                return new VectorOrError(
                    Vector.fromBytes(HASHER.hashString(value, UTF_8).asBytes()));
              }
            })
        .collect(Collectors.toList());
  }

  @Override
  public void updateConfig(EmbeddingModelConfig.ConsolidatedWorkloadParams workloadParams) {}
}
