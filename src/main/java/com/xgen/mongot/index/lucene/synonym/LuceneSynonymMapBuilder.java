package com.xgen.mongot.index.lucene.synonym;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.synonym.SynonymDocument;
import com.xgen.mongot.index.synonym.SynonymMapping;
import com.xgen.mongot.index.synonym.SynonymMappingException;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.bson.BsonValue;

/**
 * A {@link SynonymMapping.Builder} builds {@link SynonymMapping}s from a series of {@link
 * SynonymDocument}s.
 *
 * <p>This class is responsible for creating {@link SynonymMap}s, the lucene artifact representing a
 * set of analyzed words mapped to their synonyms. {@link SynonymMap} instances are immutable, and
 * cannot be incrementally modified after instantiation.
 */
public class LuceneSynonymMapBuilder implements SynonymMapping.Builder {
  private final InternalBuilder builder;
  private final Analyzer baseAnalyzer;
  private final String baseAnalyzerName;

  private LuceneSynonymMapBuilder(
      InternalBuilder builder, Analyzer analyzer, String baseAnalyzerName) {
    this.builder = builder;
    this.baseAnalyzer = analyzer;
    this.baseAnalyzerName = baseAnalyzerName;
  }

  @VisibleForTesting
  public static LuceneSynonymMapBuilder builder(Analyzer baseAnalyzer, String baseAnalyzerName) {
    return new LuceneSynonymMapBuilder(
        new InternalBuilder(baseAnalyzer), baseAnalyzer, baseAnalyzerName);
  }

  /**
   * Add a {@link SynonymDocument} to this builder. This is the method external callers should use
   * to add synonym documents to this builder.
   *
   * @throws SynonymMappingException of type {@link SynonymMappingException.Type#INVALID_DOCUMENT}
   *     wrapping a Lucene-originated IOException when {@link SynonymMap.Parser#analyze(String,
   *     CharsRefBuilder)} throws. This can happen if a synonym is an empty string after analysis.
   *     {@link SynonymDocument} strings are validated to be non-empty; this can only happen if a
   *     token is transformed to the empty string via analysis.
   */
  @Override
  public SynonymMapping.Builder addDocument(SynonymDocument document)
      throws SynonymMappingException {
    return switch (document.getMappingType()) {
      case EQUIVALENT -> this.addEquivalent(document.getSynonyms(), document.getRawDocId());
      case EXPLICIT ->
          // Explicit documents checked to have input in SynonymDocument construction.
          //noinspection OptionalGetWithoutIsPresent
          this.addExplicit(
              document.getInput().get(), document.getSynonyms(), document.getRawDocId());
    };
  }

  /**
   * Build a {@link SynonymMapping} from this builder.
   *
   * @throws SynonymMappingException of type {@link SynonymMappingException.Type#BUILD_ERROR} if
   *     lucene encounters an error building the FST internal to the {@link SynonymMap} of this
   *     {@link SynonymMapping}. An error of this type is unexpected, though possible with memory
   *     contention or via fun undiscovered bugs in lucene's FST implementation.
   */
  @Override
  public SynonymMapping build() throws SynonymMappingException {
    SynonymMap synonymMap;
    try {
      synonymMap = this.builder.build();
    } catch (IOException e) {
      throw SynonymMappingException.failSynonymMapBuild(e);
    }

    return new SynonymMapping(
        SynonymAnalyzer.create(this.baseAnalyzer, synonymMap), this.baseAnalyzerName);
  }

  /**
   * Explicit mappings match input tokens and replace them with all alternative synonym tokens.
   *
   * <p>See <a
   * href="https://github.com/10gen/mongot/blob/master/docs/syntax/synonym-collection.md">Synonym
   * Collection Specification</a> for more details.
   */
  LuceneSynonymMapBuilder addExplicit(
      Collection<String> inputs, Collection<String> outputs, Optional<BsonValue> rawDocId)
      throws SynonymMappingException {
    var analyzedInputs = analyze(inputs, rawDocId);
    var analyzedOutputs = analyze(outputs, rawDocId);

    analyzedInputs.forEach(input -> add(input, analyzedOutputs));
    return this;
  }

  /**
   * Equivalent mappings describe a set of tokens that are equivalent to one another.
   *
   * <p>See <a
   * href="https://github.com/10gen/mongot/blob/master/docs/syntax/synonym-collection.md">Synonym
   * Collection Specification</a> for more details.
   */
  LuceneSynonymMapBuilder addEquivalent(Collection<String> synonyms, Optional<BsonValue> rawDocId)
      throws SynonymMappingException {
    List<CharsRef> analyzedSynonyms = analyze(synonyms, rawDocId);

    analyzedSynonyms.forEach(synonym -> add(synonym, analyzedSynonyms));
    return this;
  }

  private void add(CharsRef input, Collection<CharsRef> outputs) {
    outputs.forEach(output -> this.builder.add(input, output));
  }

  /**
   * Analyze strings in {@code texts} and return them as {@link CharsRef}s.
   *
   * @throws SynonymMappingException of type {@link SynonymMappingException.Type#INVALID_DOCUMENT}
   *     wrapping a Lucene-originated IOException or IllegalArgumentException when {@link
   *     SynonymMap.Parser#analyze(String, CharsRefBuilder)} throws. An IllegalArgumentException can
   *     happen if a string is empty after analysis. An IOException might happen if there is an
   *     error iterating over the input token stream.
   */
  private List<CharsRef> analyze(Collection<String> texts, Optional<BsonValue> rawDocId)
      throws SynonymMappingException {
    ArrayList<CharsRef> analyzedTexts = new ArrayList<>(texts.size());
    for (String text : texts) {
      try {
        analyzedTexts.add(this.builder.analyzeToCharRef(text));
      } catch (IOException | IllegalArgumentException e) {
        throw SynonymMappingException.invalidSynonymDocument(rawDocId, e);
      }
    }
    return analyzedTexts;
  }

  /**
   * InternalBuilder is a private class to keep the {@code parse()} method private. Only use
   * InternalBuilder to analyze synonym text in the way {@link SynonymMap} expects.
   */
  private static class InternalBuilder extends SynonymMap.Parser {
    private static final boolean DEDUPLICATE = true;
    private static final boolean INCLUDE_ORIGINAL = true;

    InternalBuilder(Analyzer analyzer) {
      super(DEDUPLICATE, analyzer);
    }

    /**
     * This method is synchronized because it ends up calling {@link
     * org.apache.lucene.analysis.synonym.SynonymMap.Builder#add(CharsRef, int, CharsRef, int,
     * boolean)}, which does not appear to be thread safe.
     */
    synchronized void add(CharsRef input, CharsRef output) {
      add(input, output, INCLUDE_ORIGINAL);
    }

    CharsRef analyzeToCharRef(String text) throws IOException {
      return this.analyze(text, new CharsRefBuilder());
    }

    @Override
    public void parse(Reader in) {
      throw new AssertionError("unused");
    }
  }
}
