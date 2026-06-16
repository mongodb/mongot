package com.xgen.mongot.index.lucene.query.sort;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.sort.common.ExplainSortField;
import com.xgen.mongot.index.lucene.query.sort.mixed.MqlMixedSort;
import com.xgen.mongot.index.lucene.searcher.FieldToSortableTypesMapping;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.sort.MetaSortOptions;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.query.sort.SortBetaV1;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneSortFactory {
  private static final Logger logger = LoggerFactory.getLogger(LuceneSortFactory.class);

  private final SearchQueryFactoryContext context;

  public LuceneSortFactory(SearchQueryFactoryContext context) {
    this.context = context;
  }

  public void validateSortSpec(SortSpec sortSpec, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    for (var sortField : sortSpec.getSortFields()) {
      switch (sortSpec) {
        case Sort sort -> this.validate(sortField, embeddedRoot);
        case SortBetaV1 sortBetaV1 -> this.validateBeta(sortField);
      }
    }
  }

  private void validate(MongotSortField sortField, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    if (sortField.options() instanceof MetaSortOptions
        || this.context.getTokenFieldDefinition(sortField.field(), embeddedRoot).isPresent()) {
      return;
    }

    IndexCapabilities indexCapabilities = this.context.getIndexCapabilities();

    if (embeddedRoot.isEmpty() || indexCapabilities.supportsEmbeddedNumericAndDateV2()) {
      if (this.context.getNumericFieldDefinition(sortField.field(), embeddedRoot).isPresent()
          || this.context.getDateFieldDefinition(sortField.field(), embeddedRoot).isPresent()) {
        return;
      }
    }

    if (this.context.getUuidFieldDefinition(sortField.field(), embeddedRoot).isPresent()
        || this.context.getQueryTimeMappingChecks().indexesNulls(sortField.field(), embeddedRoot)) {
      return;
    }

    if (indexCapabilities.supportsObjectIdAndBooleanDocValues()) {
      if (this.context.getObjectIdFieldDefinition(sortField.field(), embeddedRoot).isPresent()
          || this.context.getBooleanFieldDefinition(sortField.field(), embeddedRoot).isPresent()) {
        return;
      }
    }

    if (embeddedRoot.isPresent()) {
      throw new InvalidQueryException(
          String.format(
              "Field %s under return scope %s is not indexed as sortable",
              sortField.field(), embeddedRoot.get()));
    } else {
      throw new InvalidQueryException(
          String.format("%s is not indexed as sortable", sortField.field()));
    }
  }

  private void validateBeta(MongotSortField sortField) throws InvalidQueryException {
    if (this.context.getSortableDateFieldDefinition(sortField.field()).isPresent()
        || this.context.getSortableNumberFieldDefinition(sortField.field()).isPresent()
        || this.context.getSortableStringFieldDefinition(sortField.field()).isPresent()) {
      return;
    }

    throw new InvalidQueryException(
        String.format("%s is not indexed as sortable", sortField.field()));
  }

  /**
   * Determines whether a sort field needs a prepended $nullness field for correct null/missing
   * ordering in index sort. Checks the index sort definition (not per-shard data) to ensure
   * consistent expansion across all shards in a sharded cluster.
   */
  private static boolean needsNullnessField(
      Optional<org.apache.lucene.search.Sort> indexSort, MongotSortField field) {
    if (indexSort.isEmpty() || !(field.options() instanceof UserFieldSortOptions)) {
      return false;
    }
    String nullnessFieldName = FieldName.getNullnessFieldName(field.field());
    for (SortField sortField : indexSort.get().getSort()) {
      if (nullnessFieldName.equals(sortField.getField())) {
        return true;
      }
    }
    return false;
  }

  private record ExpandedSortFields(
      List<MongotSortField> fields, ImmutableSet<Long> nullnessFieldIndexes) {}

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public org.apache.lucene.search.Sort createLuceneSort(
      SortSpec sortSpec,
      Optional<SequenceToken> sequenceToken,
      FieldToSortableTypesMapping fieldsToSortableTypesMapping,
      Optional<FieldPath> embeddedRoot,
      Optional<org.apache.lucene.search.Sort> indexSort)
      throws InvalidQueryException {

    ExpandedSortFields expandedSortFields =
        expandWithNullnessFields(sortSpec, indexSort);
    List<MongotSortField> expandedFields = expandedSortFields.fields();

    if (sequenceToken.isPresent()) {
      int tokenFieldCount = sequenceToken.get().fieldDoc().fields.length;
      int sortFieldCount = expandedFields.size();
      if (tokenFieldCount != sortFieldCount) {
        throw new InvalidQueryException(
            String.format(
                "Sequence token is incompatible with sort specification: "
                    + "expected %d sort fields but token contains %d",
                sortFieldCount, tokenFieldCount));
      }
    }

    Optional<SortFeatureExplainer> sortFeatureExplainer =
        Explain.getQueryInfo()
            .map(
                queryInfo ->
                    queryInfo.getFeatureExplainer(
                        SortFeatureExplainer.class,
                        () ->
                            new SortFeatureExplainer(
                                sortSpec,
                                fieldsToSortableTypesMapping.getFieldToSortableTypes(
                                    embeddedRoot))));

    Optional<FieldDoc> after = sequenceToken.map(SequenceToken::fieldDoc);

    boolean hasIndexSort = indexSort.isPresent();

    SortField[] sortFields =
        Streams.mapWithIndex(
                expandedFields.stream(),
                (field, idx) ->
                    expandedSortFields.nullnessFieldIndexes().contains(idx)
                        ? createNullnessSortField(field)
                        : createLuceneSortField(
                            field,
                            Optional.of(
                                fieldsToSortableTypesMapping
                                    .getFieldToSortableTypes(embeddedRoot)
                                    .get(field.field())),
                            after.map(fieldDoc -> fieldDoc.fields[(int) idx]),
                            embeddedRoot,
                            hasIndexSort))
            .map(
                sortField ->
                    Explain.isEnabled()
                        ? new ExplainSortField(sortField, sortFeatureExplainer.get())
                        : sortField)
            .toArray(SortField[]::new);

    org.apache.lucene.search.Sort luceneSort = new org.apache.lucene.search.Sort(sortFields);
    if (sortFeatureExplainer.isPresent()) {
      indexSort.ifPresent(
          is ->
              sortFeatureExplainer
                  .get()
                  .setUsesIndexSort(
                      IndexSortUtils.usesIndexSort(luceneSort, is)));
    }
    return luceneSort;
  }

  /**
   * Expands the sort spec with $nullness MongotSortField entries for INT64/Date fields that need
   * correct null/missing ordering in a sorted index. Each nullness entry is inserted immediately
   * before its corresponding user sort field.
   *
   * <p>Expansion is skipped when the expanded sort would not align with the index sort, avoiding
   * unnecessary {@link org.apache.lucene.search.Pruning#NONE} overhead on the nullness comparator.
   */
  private ExpandedSortFields expandWithNullnessFields(
      SortSpec sortSpec,
      Optional<org.apache.lucene.search.Sort> indexSort) {
    var mongotFields = sortSpec.getSortFields();
    if (indexSort.isEmpty()
        || !IndexSortUtils.expandedSortAlignsWithIndexSort(
            mongotFields, indexSort.get())) {
      return new ExpandedSortFields(mongotFields, ImmutableSet.of());
    }

    List<MongotSortField> expanded = new ArrayList<>();
    ImmutableSet.Builder<Long> nullnessFieldIndexes = ImmutableSet.builder();

    for (MongotSortField field : mongotFields) {
      if (needsNullnessField(indexSort, field)) {
        FieldPath nullnessPath = FieldPath.newRoot(FieldName.getNullnessFieldName(field.field()));
        nullnessFieldIndexes.add((long) expanded.size());
        expanded.add(new MongotSortField(nullnessPath, field.options()));
      }

      expanded.add(field);
    }

    return new ExpandedSortFields(expanded, nullnessFieldIndexes.build());
  }

  /**
   * Creates a $nullness query sort field that matches the corresponding field in the index sort.
   * This enables {@link IndexSortUtils#usesIndexSort} prefix matching for early
   * termination and sort pruning optimizations.
   */
  public static SortField createNullnessSortField(MongotSortField mongotSortField) {
    return new MqlNullnessSortField(mongotSortField);
  }

  private SortField createLuceneSortField(
      MongotSortField mongotSortField,
      Optional<ImmutableSet<FieldName.TypeField>> typeFields,
      Optional<Object> afterFieldValue,
      Optional<FieldPath> embeddedRoot,
      boolean indexSorted) {
    ImmutableSet<FieldName.TypeField> rawTypeFields =
        typeFields.orElseGet(ImmutableSet::of);
    return mongotSortField.options() instanceof MetaSortOptions metaOptions
        ? metaOptions.getMetaSort()
        : createOptimizedSortField(
                mongotSortField,
                rawTypeFields,
                afterFieldValue,
                embeddedRoot,
                this.context.getIndexCapabilities(),
                indexSorted)
            .orElseGet(() -> new MqlMixedSort(mongotSortField, embeddedRoot));
  }

  /**
   * This method either creates an optimized sort field or returns an Optional.empty(). The
   * conditions under which an optimized sort field is returned is:
   *
   * <ul>
   *   <li>Exactly 1 sortable type is present for the field in the lucene index.
   *   <li>Verify that the Lucene field type which the optimized sort is to be performed over is
   *       supported by this mongot's index feature version. See below for more information.
   *   <li>The afterFieldValue (last sort value from the previous batch) specified is either not
   *       present or is of the same type as the data present in the index for the field.
   * </ul>
   *
   * <p>In the {@link LuceneSortFactory#validate(MongotSortField, Optional)} method above, we
   * validate based on the field definitions present in the index definition, but we must also
   * verify that we aren't performing an optimized sort over if the documents in the shard do not
   * contain a valid sortable type with respect to the index feature version on this mongot.
   *
   * <p>Example: A field is configured to be indexed as token and boolean types, and the index
   * feature version is 2, i.e., boolean sort is unsupported. {@link
   * LuceneSortFactory#validate(MongotSortField, Optional)} above succeeds validation because the
   * field includes a token field definition, which is supported in IFV 2. However, suppose the
   * shard happens to contain only documents where the field is boolean-valued. As a result, the
   * {@code typeFields} map only contains {@link FieldName.TypeField#BOOLEAN}. The validation in
   * this method ensures that the mongot isn't performing an optimized sort over the boolean field
   * via {@link MqlSortedSetSortField} because IFV 2 doesn't index documents with support for
   * boolean sort, and would error if an {@link MqlSortedSetSortField} were to occur.
   */
  public static Optional<SortField> createOptimizedSortField(
      MongotSortField mongotSortField,
      ImmutableSet<FieldName.TypeField> typeFields,
      Optional<Object> afterFieldValue,
      Optional<FieldPath> embeddedRoot,
      IndexCapabilities indexCapabilities,
      boolean indexSorted) {
    if (typeFields.size() != 1) {
      return Optional.empty();
    }

    FieldName.TypeField typeField = typeFields.iterator().next();
    switch (typeField) {
      case TOKEN -> {
        return afterFieldValue.isEmpty() || afterFieldValue.get() instanceof BsonString
            ? Optional.of(
                MqlSortedSetSortField.stringSort(
                    FieldName.TypeField.TOKEN, mongotSortField, true, embeddedRoot, indexSorted))
            : Optional.empty();
      }
      case DATE_V2 -> {
        return afterFieldValue.isEmpty() || afterFieldValue.get() instanceof BsonDateTime
            ? Optional.of(
                new MqlDateSort(
                    FieldName.TypeField.DATE_V2, mongotSortField, true, embeddedRoot, indexSorted))
            : Optional.empty();
      }
      case NUMBER_DOUBLE_V2 -> {
        return afterFieldValue.isEmpty() || afterFieldValue.get() instanceof BsonDouble
            ? Optional.of(
                new MqlDoubleSort(
                    FieldName.TypeField.NUMBER_DOUBLE_V2,
                    mongotSortField,
                    true,
                    embeddedRoot,
                    indexSorted))
            : Optional.empty();
      }
      case NUMBER_INT64_V2 -> {
        return afterFieldValue.isEmpty() || afterFieldValue.get() instanceof BsonInt64
            ? Optional.of(
                new MqlLongSort(
                    FieldName.TypeField.NUMBER_INT64_V2,
                    mongotSortField,
                    true,
                    embeddedRoot,
                    indexSorted))
            : Optional.empty();
      }
      case UUID -> {
        return afterFieldValue.isEmpty()
                || (afterFieldValue.get() instanceof BsonBinary
                    && ((BsonBinary) afterFieldValue.get()).getType()
                        == BsonBinarySubType.UUID_STANDARD.getValue())
            ? Optional.of(
                MqlSortedSetSortField.uuidSort(mongotSortField, embeddedRoot, indexSorted))
            : Optional.empty();
      }
      case NULL -> {
        // MixedSort handles this case optimally
        return Optional.empty();
      }
      case OBJECT_ID -> {
        return indexCapabilities.supportsObjectIdAndBooleanDocValues()
                && (afterFieldValue.isEmpty() || afterFieldValue.get() instanceof BsonObjectId)
            ? Optional.of(
                MqlSortedSetSortField.objectIdSort(mongotSortField, embeddedRoot, indexSorted))
            : Optional.empty();
      }
      case BOOLEAN -> {
        return indexCapabilities.supportsObjectIdAndBooleanDocValues()
                && (afterFieldValue.isEmpty() || afterFieldValue.get() instanceof BsonBoolean)
            ? Optional.of(
                MqlSortedSetSortField.booleanSort(mongotSortField, embeddedRoot, indexSorted))
            : Optional.empty();
      }

      /*
       Creates a SortField for sortBeta. We are explicitly disabling pruning here in order to
       not add new functionality to a feature that is planned to be deprecated.
      */
      case SORTABLE_DATE_BETA_V1 -> {
        Check.isEmpty(embeddedRoot, "returnScope");
        return Optional.of(
            new MqlDateSort(
                FieldName.TypeField.SORTABLE_DATE_BETA_V1,
                mongotSortField,
                false,
                Optional.empty(),
                false));
      }
      case SORTABLE_NUMBER_BETA_V1 -> {
        Check.isEmpty(embeddedRoot, "returnScope");
        return Optional.of(
            new MqlDoubleSort(
                FieldName.TypeField.SORTABLE_NUMBER_BETA_V1,
                mongotSortField,
                false,
                Optional.empty(),
                false));
      }
      case SORTABLE_STRING_BETA_V1 -> {
        Check.isEmpty(embeddedRoot, "returnScope");
        return Optional.of(
            MqlSortedSetSortField.stringSort(
                FieldName.TypeField.SORTABLE_STRING_BETA_V1,
                mongotSortField,
                false,
                Optional.empty(),
                false));
      }
      case AUTOCOMPLETE,
          DATE,
          DATE_MULTIPLE,
          DATE_FACET,
          GEO_POINT,
          GEO_SHAPE,
          KNN_VECTOR,
          KNN_BYTE,
          KNN_BIT,
          KNN_F32_Q7,
          KNN_F32_Q1,
          NUMBER_DOUBLE,
          NUMBER_DOUBLE_MULTIPLE,
          NUMBER_DOUBLE_FACET,
          NUMBER_INT64,
          NUMBER_INT64_MULTIPLE,
          NUMBER_INT64_FACET,
          STRING -> {
        // do nothing
      }
    }
    logger.error("Unexpected IndexedType: {}", typeField);
    return Check.unreachable("Unexpected IndexedType");
  }
}
