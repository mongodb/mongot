package com.xgen.mongot.index.lucene.explain.knn;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.LuceneIndexSearcherReference;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.junit.Assert;
import org.junit.Test;

public class ReachabilityCheckerTest {

  @Test
  public void identifyUnreachable_runsWithDocumentsPresent_reachableDocumentsIdentifiedCorrectly()
      throws IOException {

    FieldPath fieldPath = FieldPath.newRoot("vectors");
    IndexWriter indexWriter =
        new IndexWriter(
            new ByteBuffersDirectory(),
            new IndexWriterConfig()
                .setCodec(
                    new LuceneCodec(
                        Map.of(
                            fieldPath,
                            new VectorFieldSpecification(
                                2,
                                VectorSimilarity.COSINE,
                                VectorQuantization.BINARY,
                                new VectorIndexingAlgorithm.HnswIndexingAlgorithm())))));

    String vectorFieldName =
        FieldName.TypeField.KNN_F32_Q1.getLuceneFieldName(fieldPath, Optional.empty());

    // insert documents into Lucene
    List<float[]> firstSegmentDocs =
        List.of(new float[] {0.4f, 0.5f}, new float[] {0.5f, 0.5f}, new float[] {0.6f, 0.6f});
    indexDocumentsAndCommit(indexWriter, vectorFieldName, firstSegmentDocs);

    List<float[]> secondSegmentDocs =
        List.of(new float[] {0.4f, 0.6f}, new float[] {0.5f, 0.4f}, new float[] {0.6f, 0.5f});
    indexDocumentsAndCommit(indexWriter, vectorFieldName, secondSegmentDocs);

    var searcherManager =
        LuceneSearcherManager.create(
            indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);

    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    List<LeafReaderContext> leaves = searcherReference.getIndexSearcher().getIndexReader().leaves();

    // There has to be exactly 2 leaves
    assertThat(leaves.size()).isEqualTo(2);
    for (LeafReaderContext leaf : leaves) {
      assertNotTooManyUnreachableNodes(leaf, vectorFieldName);
    }
  }

  @Test
  public void identifyUnreachable_runsWithFakeDocIds_unreachableDocumentsIdentifiedCorrectly()
      throws IOException {
    FieldPath fieldPath = FieldPath.newRoot("vectors");
    IndexWriter indexWriter =
        new IndexWriter(
            new ByteBuffersDirectory(),
            new IndexWriterConfig()
                .setCodec(
                    new LuceneCodec(
                        Map.of(
                            fieldPath,
                            new VectorFieldSpecification(
                                2,
                                VectorSimilarity.COSINE,
                                VectorQuantization.BINARY,
                                new VectorIndexingAlgorithm.HnswIndexingAlgorithm())))));

    String vectorFieldName =
        FieldName.TypeField.KNN_F32_Q1.getLuceneFieldName(fieldPath, Optional.empty());

    List<float[]> vectors =
        List.of(new float[] {0.4f, 0.5f}, new float[] {0.5f, 0.5f}, new float[] {0.6f, 0.6f});
    indexDocumentsAndCommit(indexWriter, vectorFieldName, vectors);

    var searcherManager =
        LuceneSearcherManager.create(
            indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);

    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    List<LeafReaderContext> leaves = searcherReference.getIndexSearcher().getIndexReader().leaves();

    Set<Integer> unreachableDocIds = Set.of(10, 11, 12);
    // There has to be exactly 2 leaves
    assertThat(leaves.size()).isEqualTo(1);
    Set<Integer> actuallyUnreachable =
        ReachabilityChecker.identifyUnreachable(leaves.get(0), vectorFieldName, unreachableDocIds);
    assertThat(actuallyUnreachable).isEqualTo(unreachableDocIds);
  }

  @Test
  public void identifyUnreachable_runWithDeletedDocuments_reachableDocumentsIdentifiedCorrectly()
      throws IOException {

    FieldPath fieldPath = FieldPath.newRoot("vectors");
    IndexWriter indexWriter =
        new IndexWriter(
            new ByteBuffersDirectory(),
            new IndexWriterConfig()
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setCodec(
                    new LuceneCodec(
                        Map.of(
                            fieldPath,
                            new VectorFieldSpecification(
                                2,
                                VectorSimilarity.COSINE,
                                VectorQuantization.BINARY,
                                new VectorIndexingAlgorithm.HnswIndexingAlgorithm())))));

    String vectorFieldName =
        FieldName.TypeField.KNN_F32_Q1.getLuceneFieldName(fieldPath, Optional.empty());

    // insert documents into Lucene
    List<float[]> firstSegmentDocs =
        List.of(new float[] {0.4f, 0.5f}, new float[] {0.5f, 0.5f}, new float[] {0.6f, 0.6f});

    for (int i = 0; i < firstSegmentDocs.size(); i++) {
      float[] vector = firstSegmentDocs.get(i);
      var document = new Document();
      document.add(new StringField("id", String.valueOf(i), Field.Store.YES));
      document.add(
          new KnnFloatVectorField(vectorFieldName, vector, VectorSimilarityFunction.EUCLIDEAN));
      indexWriter.addDocument(document);
    }
    indexWriter.commit();

    // modify index - make 1 delete and 1 update
    indexWriter.deleteDocuments(new Term("id", "1"));

    var documentToUpdate = new Document();
    documentToUpdate.add(new StringField("id", "2", Field.Store.YES));
    documentToUpdate.add(
        new KnnFloatVectorField(
            vectorFieldName, new float[] {0.7f, 0.7f}, VectorSimilarityFunction.EUCLIDEAN));

    indexWriter.updateDocument(new Term("id", "2"), documentToUpdate);
    indexWriter.commit();

    var searcherManager =
        LuceneSearcherManager.create(
            indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);

    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    List<LeafReaderContext> leaves = searcherReference.getIndexSearcher().getIndexReader().leaves();

    // There has to be exactly 2 leaves
    assertThat(leaves.size()).isEqualTo(2);
    for (LeafReaderContext leaf : leaves) {
      assertNotTooManyUnreachableNodes(leaf, vectorFieldName);
    }
  }

  @Test
  public void
      checkForUnreachableNodes_runsWithOnlyOneNodeInGraph_unreachableNodeIdentifiedCorrectly()
          throws IOException {
    HnswGraph graph = new GraphBuilder(1).addNode(1, new Node(1, List.of())).build();
    Set<Integer> unreachableNodes = ReachabilityChecker.checkForUnreachableNodes(graph, Set.of(1));
    assertThat(unreachableNodes).isEqualTo(Set.of());
  }

  @Test
  public void
      checkForUnreachableNodes_runsWithNoUnreachableNodes_unreachableNodeIdentifiedCorrectly()
          throws IOException {
    HnswGraph graph =
        new GraphBuilder(1)
            .addNode(1, new Node(1, List.of()))
            .addNode(0, new Node(1, List.of(2, 3)))
            .addNode(0, new Node(2, List.of(3)))
            .addNode(0, new Node(3, List.of(1)))
            .build();

    Set<Integer> unreachableNodes =
        ReachabilityChecker.checkForUnreachableNodes(graph, Set.of(1, 2, 3));

    assertThat(unreachableNodes).isEqualTo(Set.of());
  }

  @Test
  public void
      checkForUnreachableNodes_runsWithASingleUnreachableNode_unreachableIdentifiedCorrectly()
          throws IOException {
    HnswGraph graph =
        new GraphBuilder(1)
            .addNode(1, new Node(1, List.of()))
            .addNode(0, new Node(1, List.of(2, 3)))
            .addNode(0, new Node(2, List.of(3)))
            .addNode(0, new Node(3, List.of(1)))
            .addNode(
                0,
                // this node has links to all others, but no node has a link to it
                new Node(4, List.of(1, 2, 3)))
            .build();
    Set<Integer> unreachableNodes =
        ReachabilityChecker.checkForUnreachableNodes(graph, Set.of(1, 2, 3, 4));

    assertThat(unreachableNodes).isEqualTo(Set.of(4));
  }

  @Test
  public void
      checkForUnreachableNodes_runsWithAClusterOfUnreachable_unreachableIdentifiedCorrectly()
          throws IOException {
    Node entryNode = new Node(1, new ArrayList<>());
    GraphBuilder graphBuilder =
        new GraphBuilder(1)
            .addNode(1, entryNode)
            .addNode(1, new Node(2, List.of())) // has no connection to entry point
            .addNode(0, new Node(1, List.of(10)))
            .addNode(0, new Node(10, List.of(11)))
            .addNode(0, new Node(11, List.of(12)))
            .addNode(0, new Node(12, List.of()))
            .addNode(0, new Node(2, List.of(20)))
            .addNode(0, new Node(20, List.of(21)))
            .addNode(0, new Node(21, List.of()));
    HnswGraph graphWithUnreachableCluster = graphBuilder.build();

    Set<Integer> unreachableNodes =
        ReachabilityChecker.checkForUnreachableNodes(
            graphWithUnreachableCluster, Set.of(1, 2, 10, 11, 12, 20, 21));

    assertThat(unreachableNodes).isEqualTo(Set.of(2, 20, 21));

    // now will add a link on level 1 between nodes '1' and '2'
    entryNode.neighbors.add(2);

    Set<Integer> unreachableNodesAfterLinkWasAdded =
        ReachabilityChecker.checkForUnreachableNodes(
            graphWithUnreachableCluster, Set.of(1, 2, 10, 11, 12, 20, 21));
    assertThat(unreachableNodesAfterLinkWasAdded).isEqualTo(Set.of());
  }

  private static void assertNotTooManyUnreachableNodes(LeafReaderContext leaf, String field)
      throws IOException {
    Set<Integer> docsIdsToCheck = new HashSet<>();
    for (int docOrd = 0; docOrd < leaf.reader().maxDoc(); docOrd++) {
      int docId = leaf.docBase + docOrd;
      docsIdsToCheck.add(docId);
    }
    Set<Integer> unreachable = ReachabilityChecker.identifyUnreachable(leaf, field, docsIdsToCheck);

    // There should not be any unreachable nodes, but checking for emptiness can potentially make it
    // flaky.
    Assert.assertTrue("There are too many unreachable nodes", unreachable.size() <= 1);
  }

  private void indexDocumentsAndCommit(IndexWriter writer, String field, List<float[]> vectors)
      throws IOException {
    for (float[] vector : vectors) {
      var document = new Document();
      document.add(new KnnFloatVectorField(field, vector, VectorSimilarityFunction.EUCLIDEAN));
      writer.addDocument(document);
    }
    writer.commit();
  }

  private record Node(int id, List<Integer> neighbors) {}

  private static class GraphBuilder {
    private final int entry;
    private final List<List<Node>> levels = new ArrayList<>();

    public GraphBuilder(int entryNode) {
      this.entry = entryNode;
    }

    public GraphBuilder addNode(int level, Node node) {
      ensureLevels(level + 1);
      this.levels.get(level).add(node);
      return this;
    }

    private void ensureLevels(int size) {
      while (this.levels.size() < size) {
        this.levels.add(new ArrayList<>());
      }
    }

    public HnswGraph build() {
      return new StubHnswGraph(this.entry, this.levels);
    }
  }

  private static class StubHnswGraph extends HnswGraph {
    private final int entry;
    private final int maxNodeId;
    private final List<List<Node>> levels;
    private Iterator<Integer> iter;

    public StubHnswGraph(int entryNode, List<List<Node>> levels) {
      this.entry = entryNode;
      this.levels = levels;
      this.maxNodeId = levels.stream().flatMap(List::stream).mapToInt(Node::id).max().getAsInt();
    }

    @Override
    public int numLevels() {
      return this.levels.size();
    }

    @Override
    public int size() {
      return maxNodeId() + 1;
    }

    @Override
    public int maxNodeId() {
      return this.maxNodeId;
    }

    @Override
    public int entryNode() {
      return this.entry;
    }

    @Override
    public void seek(int level, int target) {
      for (Node n : this.levels.get(level)) {
        if (n.id() == target) {
          this.iter = n.neighbors().iterator();
          return;
        }
      }
      this.iter = Collections.emptyIterator();
    }

    @Override
    public int nextNeighbor() {
      return (this.iter != null && this.iter.hasNext())
          ? this.iter.next()
          : DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
      throw new UnsupportedOperationException("No need to implement.");
    }

    @Override
    public int neighborCount() {
      throw new UnsupportedOperationException("No need to implement.");
    }

    @Override
    public int maxConn() {
      throw new UnsupportedOperationException("No need to implement.");
    }
  }
}
