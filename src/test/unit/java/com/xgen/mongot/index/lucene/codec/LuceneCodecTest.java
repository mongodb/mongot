package com.xgen.mongot.index.lucene.codec;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BIT;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BYTE;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q1;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q7;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_VECTOR;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.NUMBER_INT64;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFilteringPostingsFormat;
import com.xgen.mongot.index.lucene.codec.flat.Float32AndByteFlatVectorsFormat;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBinaryQuantizedVectorsFormat;
import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBitVectorsFormat;
import com.xgen.mongot.index.lucene.vector.Lucene99NativeHnswVectorsFormat;
import com.xgen.mongot.index.lucene.vector.NativeHnswScalarQuantizedVectorsFormat;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.backward_codecs.lucene99.Lucene99PostingsFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class LuceneCodecTest {

  public record TestSetup(
      FieldName.TypeField typeField,
      Class<? extends KnnVectorsFormat> expectedFormatClass,
      int maxEdges,
      int numEdgeCandidates) {}

  @DataPoints
  public static final List<TestSetup> data =
      List.of(
          new TestSetup(KNN_VECTOR, Lucene99HnswVectorsFormat.class, 32, 200),
          new TestSetup(KNN_BYTE, Lucene99NativeHnswVectorsFormat.class, 128, 100),
          new TestSetup(KNN_BIT, Mongot01042HnswBitVectorsFormat.class, 512, 1000),
          new TestSetup(KNN_F32_Q7, NativeHnswScalarQuantizedVectorsFormat.class, 2, 30),
          new TestSetup(KNN_F32_Q1, Mongot01042HnswBinaryQuantizedVectorsFormat.class, 49, 66));

  @Theory
  public void getKnnVectorsFormatForField_withProvidedSetup_producesExpectedFormat(TestSetup setup)
      throws NoSuchFieldException, IllegalAccessException {

    FieldPath path = FieldPath.parse("vector");
    VectorFieldSpecification specification =
        createVectorFieldDefinition(setup.maxEdges(), setup.numEdgeCandidates());

    LuceneCodec codec = new LuceneCodec(Map.of(path, specification));
    PerFieldKnnVectorsFormat format = (PerFieldKnnVectorsFormat) codec.knnVectorsFormat();
    KnnVectorsFormat vectorsFormat =
        format.getKnnVectorsFormatForField(
            setup.typeField().getLuceneFieldName(path, Optional.empty()));

    Assert.assertTrue(setup.expectedFormatClass().isInstance(vectorsFormat));
    VectorFieldSpecification.HnswOptions hnswOptions =
        getKnnFieldConfig(setup.expectedFormatClass(), vectorsFormat);
    Assert.assertEquals(setup.maxEdges(), hnswOptions.maxEdges());
    Assert.assertEquals(setup.numEdgeCandidates(), hnswOptions.numEdgeCandidates());
  }

  @Test
  public void getKnnVectorsFormatForField_withWrongType_producesDefaultFormat() {
    FieldPath path = FieldPath.parse("vector");
    VectorFieldSpecification specification = createVectorFieldDefinition(48, 566);

    LuceneCodec codec = new LuceneCodec(Map.of(path, specification));
    PerFieldKnnVectorsFormat format = (PerFieldKnnVectorsFormat) codec.knnVectorsFormat();

    assertThrows(
        IllegalStateException.class,
        () ->
            format.getKnnVectorsFormatForField(
                NUMBER_INT64.getLuceneFieldName(path, Optional.empty())));
  }

  @Test
  public void getKnnVectorsFormatForField_withUnknownPath_producesDefaultFormat()
      throws NoSuchFieldException, IllegalAccessException {
    FieldPath path = FieldPath.parse("vector");

    LuceneCodec codec = new LuceneCodec();
    PerFieldKnnVectorsFormat format = (PerFieldKnnVectorsFormat) codec.knnVectorsFormat();
    KnnVectorsFormat vectorsFormat =
        format.getKnnVectorsFormatForField(KNN_VECTOR.getLuceneFieldName(path, Optional.empty()));

    Assert.assertTrue(vectorsFormat instanceof Lucene99HnswVectorsFormat);
    VectorFieldSpecification.HnswOptions hnswOptions =
        getKnnFieldConfig(Lucene99HnswVectorsFormat.class, vectorsFormat);
    Assert.assertEquals(DEFAULT_MAX_CONN, hnswOptions.maxEdges());
    Assert.assertEquals(DEFAULT_BEAM_WIDTH, hnswOptions.numEdgeCandidates());
  }

  @Test
  public void getKnnVectorsFormatForField_withUnresolvedTypeField_producesDefaultFormat() {
    FieldPath path = FieldPath.parse("vector");
    VectorFieldSpecification specification = createVectorFieldDefinition(48, 566);

    LuceneCodec codec = new LuceneCodec(Map.of(path, specification));
    PerFieldKnnVectorsFormat format = (PerFieldKnnVectorsFormat) codec.knnVectorsFormat();

    assertThrows(AssertionError.class, () -> format.getKnnVectorsFormatForField(path.toString()));
  }

  private VectorFieldSpecification createVectorFieldDefinition(
      int maxEdges, int numEdgeCandidates) {
    return new VectorFieldSpecification(
        1024,
        VectorSimilarity.DOT_PRODUCT,
        VectorQuantization.NONE,
        new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
            new VectorFieldSpecification.HnswOptions(maxEdges, numEdgeCandidates)));
  }

  private VectorFieldSpecification.HnswOptions getKnnFieldConfig(
      Class<? extends KnnVectorsFormat> knnFormatClass, KnnVectorsFormat format)
      throws NoSuchFieldException, IllegalAccessException {

    Field maxConnField = knnFormatClass.getDeclaredField("maxConn");
    maxConnField.setAccessible(true);
    int maxConn = (int) maxConnField.get(format);

    Field beamWidthField = knnFormatClass.getDeclaredField("beamWidth");
    beamWidthField.setAccessible(true);
    int beamWidth = (int) beamWidthField.get(format);

    return new VectorFieldSpecification.HnswOptions(maxConn, beamWidth);
  }

  @Test
  public void
      getKnnVectorsFormatForField_withFlatAlgo_producesFloat32AndByteFlatFormat_forKnnVector() {
    FieldPath path = FieldPath.parse("vector");
    VectorFieldSpecification specification = createFlatVectorFieldDefinition();

    LuceneCodec codec = new LuceneCodec(Map.of(path, specification));
    PerFieldKnnVectorsFormat format = (PerFieldKnnVectorsFormat) codec.knnVectorsFormat();
    KnnVectorsFormat vectorsFormat =
        format.getKnnVectorsFormatForField(KNN_VECTOR.getLuceneFieldName(path, Optional.empty()));

    Assert.assertTrue(vectorsFormat instanceof Float32AndByteFlatVectorsFormat);
  }

  @Test
  public void
      getKnnVectorsFormatForField_withFlatAlgo_producesFloat32AndByteFlatFormat_forKnnByte() {
    FieldPath path = FieldPath.parse("vector");
    VectorFieldSpecification specification = createFlatVectorFieldDefinition();

    LuceneCodec codec = new LuceneCodec(Map.of(path, specification));
    PerFieldKnnVectorsFormat format = (PerFieldKnnVectorsFormat) codec.knnVectorsFormat();
    KnnVectorsFormat vectorsFormat =
        format.getKnnVectorsFormatForField(KNN_BYTE.getLuceneFieldName(path, Optional.empty()));

    Assert.assertTrue(vectorsFormat instanceof Float32AndByteFlatVectorsFormat);
  }

  @Test
  public void postingsFormat_idField_bloomFilterEnabled_returnsBloomFilterPostingsFormat() {
    testPostingsFormat(
        true,
        FieldName.MetaField.ID.getLuceneFieldName(),
        MongotBloomFilteringPostingsFormat.class);
  }

  @Test
  public void postingsFormat_idField_bloomFilterDisabled_returnsDefaultPostingsFormat() {
    testPostingsFormat(
        false, FieldName.MetaField.ID.getLuceneFieldName(), Lucene99PostingsFormat.class);
  }

  @Test
  public void postingsFormat_nonIdField_bloomFilterEnabled_returnsDefaultPostingsFormat() {
    testPostingsFormat(true, "someOtherField", Lucene99PostingsFormat.class);
  }

  @Test
  public void postingsFormat_idField_dynamicToggle_switchesFormatAtRuntime() {
    AtomicBoolean bloomEnabled = new AtomicBoolean(false);
    LuceneCodec codec =
        LuceneCodec.Factory.forIndexWithBloomFilter(Map.of(), bloomEnabled::get, Optional.empty());
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codec.postingsFormat();
    String idField = FieldName.MetaField.ID.getLuceneFieldName();

    assertThat(format.getPostingsFormatForField(idField))
        .isInstanceOf(Lucene99PostingsFormat.class);

    bloomEnabled.set(true);
    assertThat(format.getPostingsFormatForField(idField))
        .isInstanceOf(MongotBloomFilteringPostingsFormat.class);

    bloomEnabled.set(false);
    assertThat(format.getPostingsFormatForField(idField))
        .isInstanceOf(Lucene99PostingsFormat.class);
  }

  /** NOTE - Critical to performance */
  @Test
  public void postingsFormat_bloomFilterDelegate_reusesPostingsFormatFromDelegateCodec()
      throws NoSuchFieldException, IllegalAccessException {
    LuceneCodec codec =
        LuceneCodec.Factory.forIndexWithBloomFilter(Map.of(), () -> true, Optional.empty());
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codec.postingsFormat();
    String idField = FieldName.MetaField.ID.getLuceneFieldName();

    PostingsFormat bloomFormat = format.getPostingsFormatForField(idField);
    assertThat(bloomFormat).isInstanceOf(MongotBloomFilteringPostingsFormat.class);

    Field delegateField =
        MongotBloomFilteringPostingsFormat.class.getDeclaredField("delegatePostingsFormat");
    delegateField.setAccessible(true);
    PostingsFormat bloomDelegate = (PostingsFormat) delegateField.get(bloomFormat);

    PostingsFormat defaultFormat = format.getPostingsFormatForField("someOtherField");

    assertThat(bloomDelegate).isSameInstanceAs(defaultFormat);
  }

  @Test
  public void postingsFormat_idField_withMetricsUpdater_incrementsBloomPostingCounter() {
    var metricsUpdater = SearchIndex.mockIndexingMetricsUpdater(IndexDefinition.Type.SEARCH);
    LuceneCodec codec =
        LuceneCodec.Factory.forIndexWithBloomFilter(
            Map.of(), () -> true, Optional.of(metricsUpdater));
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codec.postingsFormat();
    String idField = FieldName.MetaField.ID.getLuceneFieldName();

    assertThat(metricsUpdater.getBloomFilterIdPostingCreatedCounter().count()).isEqualTo(0.0);
    PostingsFormat bloomFormat = format.getPostingsFormatForField(idField);
    assertThat(bloomFormat).isInstanceOf(MongotBloomFilteringPostingsFormat.class);
    assertThat(metricsUpdater.getBloomFilterIdPostingCreatedCounter().count()).isEqualTo(1.0);
    assertEquals(MongotBloomFilteringPostingsFormat.BLOOM_CODEC_NAME, bloomFormat.getName());
  }

  @Test
  public void postingsFormat_idField_bloomDisabled_withMetrics_incrementsLucene99PostingCounter() {
    var metricsUpdater = SearchIndex.mockIndexingMetricsUpdater(IndexDefinition.Type.SEARCH);
    LuceneCodec codec =
        LuceneCodec.Factory.forIndexWithBloomFilter(
            Map.of(), () -> false, Optional.of(metricsUpdater));
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codec.postingsFormat();
    String idField = FieldName.MetaField.ID.getLuceneFieldName();

    assertThat(metricsUpdater.getLucene99IdPostingCreatedCounter().count()).isEqualTo(0.0);
    PostingsFormat defaultFormat = format.getPostingsFormatForField(idField);
    assertThat(defaultFormat).isInstanceOf(Lucene99PostingsFormat.class);
    assertThat(metricsUpdater.getLucene99IdPostingCreatedCounter().count()).isEqualTo(1.0);
    assertThat(defaultFormat).isNotInstanceOf(MongotBloomFilteringPostingsFormat.class);
    assertThat(defaultFormat.getName())
        .isNotEqualTo(MongotBloomFilteringPostingsFormat.BLOOM_CODEC_NAME);
  }

  private static void testPostingsFormat(
      boolean isBloomEnabled, String fieldName, Class<? extends PostingsFormat> expectedType) {
    LuceneCodec codec =
        LuceneCodec.Factory.forIndexWithBloomFilter(
            Map.of(), () -> isBloomEnabled, Optional.empty());
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codec.postingsFormat();

    PostingsFormat idFormat = format.getPostingsFormatForField(fieldName);

    assertThat(idFormat).isInstanceOf(expectedType);
  }

  private VectorFieldSpecification createFlatVectorFieldDefinition() {
    return new VectorFieldSpecification(
        1024,
        VectorSimilarity.DOT_PRODUCT,
        VectorQuantization.NONE,
        new VectorIndexingAlgorithm.FlatIndexingAlgorithm());
  }
}
