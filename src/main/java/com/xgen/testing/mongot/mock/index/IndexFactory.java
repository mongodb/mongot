package com.xgen.testing.mongot.mock.index;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.mockito.Answers;
import org.mockito.stubbing.Answer;

public class IndexFactory {

  /**
   * Creates a mocked IndexFactory for use in tests.
   *
   * @param createdIndexesObserver callback any time an index is created.
   * @param statusForCreatedIndexes supplies status to set on an index when created.
   */
  public static com.xgen.mongot.index.IndexFactory mockIndexFactory(
      Consumer<IndexGeneration> createdIndexesObserver,
      Supplier<IndexStatus> statusForCreatedIndexes)
      throws Exception {
    return mockIndexFactory(
        com.xgen.mongot.index.IndexFactory.class, createdIndexesObserver, statusForCreatedIndexes);
  }

  /** Generic version of mockIndexFactory, to support any subclass of IndexFactory. */
  public static <T extends com.xgen.mongot.index.IndexFactory> T mockIndexFactory(
      Class<T> classType,
      Consumer<IndexGeneration> createdIndexesObserver,
      Supplier<IndexStatus> statusForCreatedIndexes)
      throws Exception {
    return mockIndexFactory(
        classType, createdIndexesObserver, statusForCreatedIndexes, Answers.RETURNS_DEFAULTS);
  }

  /** Generic version with a custom answer for unstubbed methods. */
  public static <T extends com.xgen.mongot.index.IndexFactory> T mockIndexFactory(
      Class<T> classType,
      Consumer<IndexGeneration> createdIndexesObserver,
      Supplier<IndexStatus> statusForCreatedIndexes,
      Answer<?> defaultAnswer)
      throws Exception {
    T indexFactory = mock(classType, defaultAnswer);

    Answer<Index> getIndexAnswer =
        invocation -> {
          // create a mock index and notify the observer
          IndexDefinitionGeneration defGen = invocation.getArgument(0);

          Index index =
              switch (defGen.getType()) {
                case SEARCH -> SearchIndex.mockIndex(defGen.asSearch());
                case VECTOR -> VectorIndex.mockIndex(defGen.asVector());
                case AUTO_EMBEDDING -> MaterializedViewIndex.mockIndex(defGen.asMaterializedView());
              };

          index.setStatus(statusForCreatedIndexes.get());
          createdIndexesObserver.accept(new IndexGeneration(index, defGen));
          return index;
        };

    when(indexFactory.getIndex(any(IndexDefinitionGeneration.class))).thenAnswer(getIndexAnswer);
    when(indexFactory.getInitializedIndex(any(), any()))
        .thenAnswer(
            invocation -> {
              Index index = invocation.getArgument(0);
              IndexDefinitionGeneration defGen = invocation.getArgument(1);
              return switch (defGen.getType()) {
                case SEARCH ->
                    SearchIndex.mockInitializedIndex(
                        index.asSearchIndex(), defGen.getGenerationId());
                case VECTOR ->
                    VectorIndex.mockInitializedIndex(
                        index.asVectorIndex(), defGen.getGenerationId());
                case AUTO_EMBEDDING -> MaterializedViewIndex.mockIndex(defGen.asMaterializedView());
              };
            });

    return indexFactory;
  }
}
