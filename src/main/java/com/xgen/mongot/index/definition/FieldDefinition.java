package com.xgen.mongot.index.definition;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.Enums;
import com.xgen.mongot.util.Optionals;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonValue;

/** FieldDefinition definition how different data types should be indexed for a given field. */
public record FieldDefinition(
    Optional<AutocompleteFieldDefinition> autocompleteFieldDefinition,
    Optional<BooleanFieldDefinition> booleanFieldDefinition,
    Optional<DateFieldDefinition> dateFieldDefinition,
    Optional<DateFacetFieldDefinition> dateFacetFieldDefinition,
    Optional<DocumentFieldDefinition> documentFieldDefinition,
    Optional<EmbeddedDocumentsFieldDefinition> embeddedDocumentsFieldDefinition,
    Optional<GeoFieldDefinition> geoFieldDefinition,
    Optional<SearchAutoEmbedFieldDefinition> searchAutoEmbedFieldDefinition,
    Optional<SearchIndexVectorFieldDefinition> searchIndexVectorFieldDefinition,
    Optional<KnnVectorFieldDefinition> knnVectorFieldDefinition,
    Optional<NumberFieldDefinition> numberFieldDefinition,
    Optional<NumberFacetFieldDefinition> numberFacetFieldDefinition,
    Optional<ObjectIdFieldDefinition> objectIdFieldDefinition,
    Optional<SortableDateBetaV1FieldDefinition> sortableDateBetaV1FieldDefinition,
    Optional<SortableNumberBetaV1FieldDefinition> sortableNumberBetaV1FieldDefinition,
    Optional<SortableStringBetaV1FieldDefinition> sortableStringBetaV1FieldDefinition,
    Optional<StringFieldDefinition> stringFieldDefinition,
    Optional<StringFacetFieldDefinition> stringFacetFieldDefinition,
    Optional<TokenFieldDefinition> tokenFieldDefinition,
    Optional<UuidFieldDefinition> uuidFieldDefinition)
    implements Encodable {

  private static final Value.Required<List<FieldTypeDefinition>> TYPE_VALUE =
      Value.builder()
          .classValue(FieldTypeDefinition::fromBson)
          .disallowUnknownFields()
          .asSingleValueOrList()
          .required();

  /**
   * This is the FieldDefinition to use for a dynamically discovered field.
   *
   * <p>It should continue to dynamically index children, and index values with the default values
   * provided by the IndexDefinition.
   *
   * <p>Note: Use {@link
   * SearchFieldDefinitionResolver#getDynamicFieldDefinition(HierarchicalFieldDefinition)} instead
   * of directly referencing this static object.
   */
  static final FieldDefinition DYNAMIC_FIELD_DEFINITION =
      new FieldDefinition(
          Optional.empty(),
          Optional.of(new BooleanFieldDefinition()),
          Optional.of(new DateFieldDefinition()),
          Optional.empty(),
          Optional.of(createOrAssert(new DynamicDefinition.Boolean(true), Collections.emptyMap())),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.of(
              new NumberFieldDefinition(
                  new NumericFieldOptions(
                      NumericFieldOptions.Fields.REPRESENTATION.getDefaultValue(),
                      NumericFieldOptions.Fields.INDEX_DOUBLES.getDefaultValue(),
                      NumericFieldOptions.Fields.INDEX_INTEGERS.getDefaultValue()))),
          Optional.empty(),
          Optional.of(new ObjectIdFieldDefinition()),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.of(
              StringFieldDefinition.create(
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  StringFieldDefinition.Fields.INDEX_OPTIONS.getDefaultValue(),
                  StringFieldDefinition.Fields.STORE.getDefaultValue(),
                  StringFieldDefinition.Fields.NORMS.getDefaultValue(),
                  Collections.emptyMap())),
          Optional.empty(),
          Optional.empty(),
          Optional.of(new UuidFieldDefinition()));

  // Types that can be indexed for indexing sorting.
  static final ImmutableSet<FieldTypeDefinition.Type> INDEXING_SORTABLE_TYPES =
      ImmutableSet.of(
          FieldTypeDefinition.Type.BOOLEAN,
          FieldTypeDefinition.Type.DATE,
          FieldTypeDefinition.Type.NUMBER,
          FieldTypeDefinition.Type.OBJECT_ID,
          FieldTypeDefinition.Type.TOKEN,
          FieldTypeDefinition.Type.UUID);

  static FieldDefinition fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    // Use a custom error message here to avoid cryptic errors in the case where a field definition
    // has a "null" mapping value in an index definition.
    //
    // Without this, attempting to deserialize an (invalid) field definition with a null value
    // results in an error message like "<path> is required", which is undesirable. For example, a
    // document field definition like:
    //
    // {
    //  "fields": {
    //    "a": null
    //  }
    // }
    //
    // would produce an error message like "fields.a is required", which is undesirable.
    if (value.isNull()) {
      context.handleSemanticError("cannot be null");
    }

    var definitions = TYPE_VALUE.getParser().parse(context, value);
    return fromUnvalidatedFieldTypeDefinitions(context, definitions);
  }

  @Override
  public BsonValue toBson() {
    List<FieldTypeDefinition> definitions =
        Optionals.present(getAllDefinitions()).collect(Collectors.toList());
    return TYPE_VALUE.getEncoder().encode(definitions);
  }

  /** Returns true if the field has any non-document definitions. */
  public boolean hasScalarFieldDefinitions() {
    return getAllDefinitions()
        .anyMatch(
            definition ->
                definition.isPresent()
                    && definition.get().getType() != FieldTypeDefinition.Type.DOCUMENT);
  }

  Stream<Optional<? extends FieldTypeDefinition>> getAllDefinitions() {
    return Stream.of(
        this.autocompleteFieldDefinition,
        this.booleanFieldDefinition,
        this.dateFieldDefinition,
        this.dateFacetFieldDefinition,
        this.documentFieldDefinition,
        this.embeddedDocumentsFieldDefinition,
        this.geoFieldDefinition,
        this.searchAutoEmbedFieldDefinition,
        this.searchIndexVectorFieldDefinition,
        this.knnVectorFieldDefinition,
        this.numberFieldDefinition,
        this.numberFacetFieldDefinition,
        this.objectIdFieldDefinition,
        this.sortableDateBetaV1FieldDefinition,
        this.sortableNumberBetaV1FieldDefinition,
        this.sortableStringBetaV1FieldDefinition,
        this.stringFieldDefinition,
        this.stringFacetFieldDefinition,
        this.tokenFieldDefinition,
        this.uuidFieldDefinition);
  }

  static FieldDefinition fromValidatedFieldTypeDefinitions(List<FieldTypeDefinition> definitions) {
    @Var Optional<AutocompleteFieldDefinition> autocompleteFieldDefinition = Optional.empty();
    @Var Optional<BooleanFieldDefinition> booleanFieldDefinition = Optional.empty();
    @Var Optional<DateFieldDefinition> dateFieldDefinition = Optional.empty();
    @Var Optional<DateFacetFieldDefinition> dateFacetFieldDefinition = Optional.empty();
    @Var Optional<DocumentFieldDefinition> documentFieldDefinition = Optional.empty();
    @Var
    Optional<EmbeddedDocumentsFieldDefinition> embeddedDocumentsFieldDefinition = Optional.empty();
    @Var Optional<GeoFieldDefinition> geoFieldDefinition = Optional.empty();
    @Var
    Optional<SearchAutoEmbedFieldDefinition> searchAutoEmbedFieldDefinition = Optional.empty();
    @Var
    Optional<SearchIndexVectorFieldDefinition> searchIndexVectorFieldDefinition = Optional.empty();
    @Var Optional<KnnVectorFieldDefinition> knnVectorFieldDefinition = Optional.empty();
    @Var Optional<NumberFieldDefinition> numberFieldDefinition = Optional.empty();
    @Var Optional<NumberFacetFieldDefinition> numberFacetFieldDefinition = Optional.empty();
    @Var Optional<ObjectIdFieldDefinition> objectIdFieldDefinition = Optional.empty();
    @Var
    Optional<SortableDateBetaV1FieldDefinition> sortableDateBetaV1FieldDefinition =
        Optional.empty();
    @Var
    Optional<SortableNumberBetaV1FieldDefinition> sortableNumberBetaV1FieldDefinition =
        Optional.empty();
    @Var
    Optional<SortableStringBetaV1FieldDefinition> sortableStringBetaV1FieldDefinition =
        Optional.empty();
    @Var Optional<StringFieldDefinition> stringFieldDefinition = Optional.empty();
    @Var Optional<StringFacetFieldDefinition> stringFacetFieldDefinition = Optional.empty();
    @Var Optional<TokenFieldDefinition> tokenFieldDefinition = Optional.empty();
    @Var Optional<UuidFieldDefinition> uuidFieldDefinition = Optional.empty();

    for (FieldTypeDefinition definition : definitions) {
      switch (definition) {
        case AutocompleteFieldDefinition fd -> autocompleteFieldDefinition = Optional.of(fd);
        case BooleanFieldDefinition fd -> booleanFieldDefinition = Optional.of(fd);
        case DateFieldDefinition fd -> dateFieldDefinition = Optional.of(fd);
        case DateFacetFieldDefinition fd -> dateFacetFieldDefinition = Optional.of(fd);
        case DocumentFieldDefinition fd -> documentFieldDefinition = Optional.of(fd);
        case EmbeddedDocumentsFieldDefinition fd ->
            embeddedDocumentsFieldDefinition = Optional.of(fd);
        case GeoFieldDefinition fd -> geoFieldDefinition = Optional.of(fd);
        case SearchAutoEmbedFieldDefinition fd ->
            searchAutoEmbedFieldDefinition = Optional.of(fd);
        case SearchIndexVectorFieldDefinition fd ->
            searchIndexVectorFieldDefinition = Optional.of(fd);
        case KnnVectorFieldDefinition fd -> knnVectorFieldDefinition = Optional.of(fd);
        case NumberFieldDefinition fd -> numberFieldDefinition = Optional.of(fd);
        case NumberFacetFieldDefinition fd -> numberFacetFieldDefinition = Optional.of(fd);
        case ObjectIdFieldDefinition fd -> objectIdFieldDefinition = Optional.of(fd);
        case SortableDateBetaV1FieldDefinition fd ->
            sortableDateBetaV1FieldDefinition = Optional.of(fd);
        case SortableNumberBetaV1FieldDefinition fd ->
            sortableNumberBetaV1FieldDefinition = Optional.of(fd);
        case SortableStringBetaV1FieldDefinition fd ->
            sortableStringBetaV1FieldDefinition = Optional.of(fd);
        case StringFieldDefinition fd -> stringFieldDefinition = Optional.of(fd);
        case StringFacetFieldDefinition fd -> stringFacetFieldDefinition = Optional.of(fd);
        case TokenFieldDefinition fd -> tokenFieldDefinition = Optional.of(fd);
        case UuidFieldDefinition fd -> uuidFieldDefinition = Optional.of(fd);
      }
    }

    return new FieldDefinition(
        autocompleteFieldDefinition,
        booleanFieldDefinition,
        dateFieldDefinition,
        dateFacetFieldDefinition,
        documentFieldDefinition,
        embeddedDocumentsFieldDefinition,
        geoFieldDefinition,
        searchAutoEmbedFieldDefinition,
        searchIndexVectorFieldDefinition,
        knnVectorFieldDefinition,
        numberFieldDefinition,
        numberFacetFieldDefinition,
        objectIdFieldDefinition,
        sortableDateBetaV1FieldDefinition,
        sortableNumberBetaV1FieldDefinition,
        sortableStringBetaV1FieldDefinition,
        stringFieldDefinition,
        stringFacetFieldDefinition,
        tokenFieldDefinition,
        uuidFieldDefinition);
  }

  private static FieldDefinition fromUnvalidatedFieldTypeDefinitions(
      BsonParseContext context, List<FieldTypeDefinition> definitions) throws BsonParseException {
    Set<FieldTypeDefinition.Type> duplicateTypes =
        definitions.stream()
            .map(FieldTypeDefinition::getType)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue() > 1)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    if (!duplicateTypes.isEmpty()) {
      context.handleSemanticError(
          String.format(
              "field contained multiple \"%s\" type definitions",
              duplicateTypes.stream()
                  .map(type -> Enums.convertNameTo(CaseFormat.LOWER_CAMEL, type))
                  .collect(Collectors.toSet())));
    }

    Function<Set<FieldTypeDefinition.Type>, Set<FieldTypeDefinition.Type>> extractMatchingTypes =
        targetTypes ->
            definitions.stream()
                .map(FieldTypeDefinition::getType)
                .filter(targetTypes::contains)
                .collect(Collectors.toSet());

    var numBetaSortable =
        extractMatchingTypes
            .apply(
                Set.of(
                    FieldTypeDefinition.Type.SORTABLE_DATE_BETA_V1,
                    FieldTypeDefinition.Type.SORTABLE_NUMBER_BETA_V1,
                    FieldTypeDefinition.Type.SORTABLE_STRING_BETA_V1))
            .size();
    if (numBetaSortable > 1) {
      String sortableTypes = "[sortableDateBetaV1, sortableNumberBetaV1, sortableStringBetaV1]";
      return context.handleSemanticError("can only define one of " + sortableTypes);
    }

    int numVectorTypes =
        extractMatchingTypes
            .apply(
                Set.of(
                    FieldTypeDefinition.Type.KNN_VECTOR,
                    FieldTypeDefinition.Type.VECTOR,
                    FieldTypeDefinition.Type.AUTO_EMBED_VECTOR))
            .size();

    if (numVectorTypes > 1) {
      String vectorTypes = "[knnVector, vector]";
      return context.handleSemanticError("can only define one of " + vectorTypes);
    }

    if (extractMatchingTypes
            .apply(
                Set.of(
                    FieldTypeDefinition.Type.SORTABLE_STRING_BETA_V1,
                    FieldTypeDefinition.Type.TOKEN))
            .size()
        > 1) {
      String sortableStringTypes = "[SortableStringBetaV1, Token]";
      return context.handleSemanticError(
          String.format("Can only define one of %s", sortableStringTypes));
    }

    return fromValidatedFieldTypeDefinitions(definitions);
  }

  /** Asserting create method used in {@link #DYNAMIC_FIELD_DEFINITION} and. */
  static DocumentFieldDefinition createOrAssert(
      DynamicDefinition dynamic, Map<String, FieldDefinition> fields) {
    try {
      return DocumentFieldDefinition.create(dynamic, fields);
    } catch (IllegalEmbeddedFieldException e) {
      throw new AssertionError(e);
    }
  }

  public Optional<VectorFieldSpecification> vectorFieldSpecification() {
    if (this.knnVectorFieldDefinition.isPresent()) {
      return this.knnVectorFieldDefinition.map(KnnVectorFieldDefinition::specification);
    }
    if (this.searchAutoEmbedFieldDefinition.isPresent()) {
      return this.searchAutoEmbedFieldDefinition.map(SearchAutoEmbedFieldDefinition::specification);
    }
    return this.searchIndexVectorFieldDefinition.map(
        SearchIndexVectorFieldDefinition::specification);
  }

  public static ImmutableSet<FieldTypeDefinition.Type> getIndexingSortableTypes() {
    return INDEXING_SORTABLE_TYPES;
  }
}
