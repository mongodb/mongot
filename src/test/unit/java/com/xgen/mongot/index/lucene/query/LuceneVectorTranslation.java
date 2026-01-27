package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Assert;

/**
 * Tests helper class which sets up a real Lucene vector index and translates mongot queries to
 * Lucene queries.
 */
class LuceneVectorTranslation {
  private final VectorIndexDefinition indexDefinition;
  private Directory directory;
  private IndexWriter writer;
  private final FeatureFlags featureFlags;

  private void setUp() throws IOException {
    var temporaryFolder = TestUtils.getTempFolder();
    this.directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    this.writer = new IndexWriter(this.directory, new IndexWriterConfig());
    this.writer.commit();
  }

  private void tearDown() throws IOException {
    this.writer.close();
    this.directory.close();
  }

  LuceneVectorTranslation(List<VectorIndexFieldDefinition> definitions) {
    this.indexDefinition = getIndexDefinition(definitions);
    this.featureFlags = FeatureFlags.getDefault();
  }

  LuceneVectorTranslation(List<VectorIndexFieldDefinition> definitions, FeatureFlags featureFlags) {
    this.indexDefinition = getIndexDefinition(definitions);
    this.featureFlags = featureFlags;
  }

  void assertTranslatedTo(VectorSearchQuery query, Query expected)
      throws InvalidQueryException, IOException {
    Query result = translate(query);
    Assert.assertEquals("Lucene query:", expected, result);
  }

  Query translate(VectorSearchQuery query) throws InvalidQueryException, IOException {
    setUp();
    return getLuceneQuery(query);
  }

  private Query getLuceneQuery(VectorSearchQuery query) throws IOException, InvalidQueryException {
    var metrics = new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    var context =
        new VectorQueryFactoryContext(
            this.indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            this.featureFlags,
            metrics);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);
    try (var reader = DirectoryReader.open(this.directory)) {
      return factory.createQuery(
          new MaterializedVectorSearchQuery(query, query.criteria().queryVector().get()), reader);
    } finally {
      tearDown();
    }
  }

  static VectorIndexDefinition getIndexDefinition(List<VectorIndexFieldDefinition> definitions) {
    return VectorIndexDefinitionBuilder.builder().setFields(definitions).build();
  }
}
