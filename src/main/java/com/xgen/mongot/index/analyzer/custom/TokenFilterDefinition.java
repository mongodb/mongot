package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public abstract class TokenFilterDefinition
    implements DocumentEncodable, TokenStreamTransformationStage {
  static class Fields {
    static final Field.Required<Type> TYPE =
        Field.builder("type").enumField(Type.class).asCamelCase().required();
  }

  public enum Type {
    ASCII_FOLDING,
    DAITCH_MOKOTOFF_SOUNDEX,
    EDGE_GRAM,
    ENGLISH_POSSESSIVE,
    FLATTEN_GRAPH,
    ICU_FOLDING,
    ICU_NORMALIZER,
    /** Duplicates every input token, but tags one of them as "keyword" to prevent stemming. */
    KEYWORD_REPEAT,
    K_STEMMING,
    LENGTH,
    LIMIT_TOKEN_COUNT,
    LOWERCASE,
    N_GRAM,
    PORTER_STEMMING,
    REGEX,
    /** Removes consecutive duplicate tokens. */
    REMOVE_DUPLICATES,
    REVERSE,
    SHINGLE,
    SNOWBALL_STEMMING,
    SPANISH_PLURAL_STEMMING,
    STEMPEL,
    STOPWORD,
    TRIM,
    WORD_DELIMITER_GRAPH,
  }

  public abstract Type getType();

  abstract BsonDocument tokenFilterToBson();

  @Override
  public abstract boolean equals(Object other);

  @Override
  public abstract int hashCode();

  @Override
  public final BsonDocument toBson() {
    BsonDocument doc = BsonDocumentBuilder.builder().field(Fields.TYPE, getType()).build();
    doc.putAll(tokenFilterToBson());
    return doc;
  }

  /** Deserialize a BSON document into a new CustomTokenFilterDefinition. */
  public static TokenFilterDefinition fromBson(DocumentParser parser) throws BsonParseException {
    var type = parser.getField(Fields.TYPE).unwrap();
    return switch (type) {
      case ASCII_FOLDING -> AsciiFoldingTokenFilterDefinition.fromBson(parser);
      case DAITCH_MOKOTOFF_SOUNDEX -> DaitchMokotoffSoundexTokenFilterDefinition.fromBson(parser);
      case EDGE_GRAM -> EdgeGramTokenFilterDefinition.fromBson(parser);
      case ENGLISH_POSSESSIVE -> new EnglishPossessiveTokenFilterDefinition();
      case FLATTEN_GRAPH -> new FlattenGraphTokenFilterDefinition();
      case ICU_FOLDING -> new IcuFoldingTokenFilterDefinition();
      case ICU_NORMALIZER -> IcuNormalizerTokenFilterDefinition.fromBson(parser);
      case KEYWORD_REPEAT -> new KeywordRepeatTokenFilterDefinition();
      case K_STEMMING -> new KStemmingTokenFilterDefinition();
      case N_GRAM -> NGramTokenFilterDefinition.fromBson(parser);
      case PORTER_STEMMING -> new PorterStemmingTokenFilterDefinition();
      case LENGTH -> LengthTokenFilterDefinition.fromBson(parser);
      case LIMIT_TOKEN_COUNT -> LimitTokenFilterDefinition.fromBson(parser);
      case LOWERCASE -> new LowercaseTokenFilterDefinition();
      case REGEX -> RegexTokenFilterDefinition.fromBson(parser);
      case REMOVE_DUPLICATES -> new RemoveDuplicatesTokenFilterDefinition();
      case REVERSE -> new ReverseTokenFilterDefinition();
      case SHINGLE -> ShingleTokenFilterDefinition.fromBson(parser);
      case SNOWBALL_STEMMING -> SnowballStemmingTokenFilterDefinition.fromBson(parser);
      case SPANISH_PLURAL_STEMMING -> new SpanishPluralStemmingTokenFilterDefinition();
      case STEMPEL -> new StempelTokenFilterDefinition();
      case STOPWORD -> StopwordTokenFilterDefinition.fromBson(parser);
      case TRIM -> new TrimTokenFilterDefinition();
      case WORD_DELIMITER_GRAPH -> WordDelimiterGraphTokenFilterDefinition.fromBson(parser);
    };
  }

  public AsciiFoldingTokenFilterDefinition asAsciiFoldingTokenFilterDefinition() {
    Check.expectedType(Type.ASCII_FOLDING, this.getType());
    return (AsciiFoldingTokenFilterDefinition) this;
  }

  public DaitchMokotoffSoundexTokenFilterDefinition asDaitchMokotoffSoundexFilterDefinition() {
    Check.expectedType(Type.DAITCH_MOKOTOFF_SOUNDEX, this.getType());
    return (DaitchMokotoffSoundexTokenFilterDefinition) this;
  }

  public EdgeGramTokenFilterDefinition asEdgeGramTokenFilterDefinition() {
    Check.expectedType(Type.EDGE_GRAM, this.getType());
    return (EdgeGramTokenFilterDefinition) this;
  }

  public EnglishPossessiveTokenFilterDefinition asEnglishPossessiveTokenFilterDefinition() {
    Check.expectedType(Type.ENGLISH_POSSESSIVE, this.getType());
    return (EnglishPossessiveTokenFilterDefinition) this;
  }

  public FlattenGraphTokenFilterDefinition asFlattenGraphTokenFilterDefinition() {
    Check.expectedType(Type.FLATTEN_GRAPH, this.getType());
    return (FlattenGraphTokenFilterDefinition) this;
  }

  public IcuFoldingTokenFilterDefinition asIcuFoldingTokenFilterDefinition() {
    Check.expectedType(Type.ICU_FOLDING, this.getType());
    return (IcuFoldingTokenFilterDefinition) this;
  }

  public IcuNormalizerTokenFilterDefinition asIcuNormalizerTokenFilterDefinition() {
    Check.expectedType(Type.ICU_NORMALIZER, this.getType());
    return (IcuNormalizerTokenFilterDefinition) this;
  }

  public KStemmingTokenFilterDefinition asKStemmingTokenFilterDefinition() {
    Check.expectedType(Type.K_STEMMING, this.getType());
    return (KStemmingTokenFilterDefinition) this;
  }

  public NGramTokenFilterDefinition asNGramTokenFilterDefinition() {
    Check.expectedType(Type.N_GRAM, this.getType());
    return (NGramTokenFilterDefinition) this;
  }

  public PorterStemmingTokenFilterDefinition asPorterStemmingTokenFilterDefinition() {
    Check.expectedType(Type.PORTER_STEMMING, this.getType());
    return (PorterStemmingTokenFilterDefinition) this;
  }

  public LengthTokenFilterDefinition asLengthTokenFilterDefinition() {
    Check.expectedType(Type.LENGTH, this.getType());
    return (LengthTokenFilterDefinition) this;
  }

  public LimitTokenFilterDefinition asLimitTokenFilterDefinition() {
    Check.expectedType(Type.LIMIT_TOKEN_COUNT, this.getType());
    return (LimitTokenFilterDefinition) this;
  }

  public LowercaseTokenFilterDefinition asLowercaseTokenFilterDefinition() {
    Check.expectedType(Type.LOWERCASE, this.getType());
    return (LowercaseTokenFilterDefinition) this;
  }

  public RegexTokenFilterDefinition asRegexTokenFilterDefinition() {
    Check.expectedType(Type.REGEX, this.getType());
    return (RegexTokenFilterDefinition) this;
  }

  public ShingleTokenFilterDefinition asShingleTokenFilterDefinition() {
    Check.expectedType(Type.SHINGLE, this.getType());
    return (ShingleTokenFilterDefinition) this;
  }

  public SnowballStemmingTokenFilterDefinition asSnowballTokenFilterDefinition() {
    Check.expectedType(Type.SNOWBALL_STEMMING, this.getType());
    return (SnowballStemmingTokenFilterDefinition) this;
  }

  public SpanishPluralStemmingTokenFilterDefinition asSpanishPluralTokenFilterDefinition() {
    Check.expectedType(Type.SPANISH_PLURAL_STEMMING, this.getType());
    return (SpanishPluralStemmingTokenFilterDefinition) this;
  }

  public StempelTokenFilterDefinition asStempelTokenFilterDefinition() {
    Check.expectedType(Type.STEMPEL, this.getType());
    return (StempelTokenFilterDefinition) this;
  }

  public StopwordTokenFilterDefinition asStopwordTokenFilterDefinition() {
    Check.expectedType(Type.STOPWORD, this.getType());
    return (StopwordTokenFilterDefinition) this;
  }

  public TrimTokenFilterDefinition asTrimTokenFilterDefinition() {
    Check.expectedType(Type.TRIM, this.getType());
    return (TrimTokenFilterDefinition) this;
  }

  public WordDelimiterGraphTokenFilterDefinition asWordDelimiterGraphTokenFilterDefinition() {
    Check.expectedType(Type.WORD_DELIMITER_GRAPH, this.getType());
    return (WordDelimiterGraphTokenFilterDefinition) this;
  }
}
