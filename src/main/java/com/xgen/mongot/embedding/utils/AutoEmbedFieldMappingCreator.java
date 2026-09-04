package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.HASH_FIELD_SUFFIX;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping.AutoEmbedField;
import com.xgen.mongot.embedding.SearchIndexAutoEmbedFieldMapping;
import com.xgen.mongot.embedding.VectorAutoEmbedFieldMapping;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.HierarchicalFieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory methods for creating {@link AutoEmbedFieldMapping} instances from index definitions. */
public class AutoEmbedFieldMappingCreator {

  private static final Logger LOG = LoggerFactory.getLogger(AutoEmbedFieldMappingCreator.class);

  private static final String HASH_FIELD_PREFIX = "_autoEmbed._hash";

  // Mirrors {@code MaterializedViewCollectionResolver.MV_COLLECTION_SCHEMA_NAMESPACE} +
  // {@link FieldPath#DELIMITER}. Inlined to avoid a util → mongodb package dependency.
  private static final String MV_NAMESPACE_PREFIX = "_autoEmbed" + FieldPath.DELIMITER;

  /**
   * Creates an {@code AutoEmbedFieldMapping} from a source {@link IndexDefinition}, dispatching to
   * the index-type-specific factory.
   */
  public static AutoEmbedFieldMapping createAutoEmbedMapping(IndexDefinition indexDefinition) {
    return switch (indexDefinition) {
      case VectorIndexDefinition v -> createAutoEmbedMapping(v);
      case SearchIndexDefinition s -> createAutoEmbedMapping(s);
    };
  }

  /** Creates an {@code AutoEmbedFieldMapping} from a source {@link VectorIndexDefinition}. */
  public static AutoEmbedFieldMapping createAutoEmbedMapping(
      VectorIndexDefinition indexDefinition) {
    VectorIndexFieldMapping mappings = indexDefinition.getMappings();
    ImmutableMap.Builder<FieldPath, AutoEmbedField> builder = ImmutableMap.builder();
    for (Map.Entry<FieldPath, VectorIndexFieldDefinition> entry : mappings.fieldMap().entrySet()) {
      FieldPath path = entry.getKey();
      VectorIndexFieldDefinition field = entry.getValue();
      switch (field.getType()) {
        case AUTO_EMBED -> {
          var spec = field.asVectorAutoEmbedField().specification();
          builder.put(path, new AutoEmbedField.EmbedField(path, spec));
        }
        case TEXT -> {
          var spec = field.asVectorTextField().specification();
          builder.put(path, new AutoEmbedField.EmbedField(path, spec));
        }
        case FILTER -> builder.put(path, new AutoEmbedField.PassthroughField(path));
        case VECTOR -> {
          // Output field written by the embedding provider, not a source field.
        }
        case EMBEDDED_DOCUMENTS -> {
          // Structural array marker, not a leaf field to process.
        }
      }
    }
    return new VectorAutoEmbedFieldMapping(builder.build());
  }

  /**
   * Creates an {@code AutoEmbedFieldMapping} from a source {@link SearchIndexDefinition}.
   *
   * <p>Auto-embed fields (keyed on {@code sourceField()}) become {@link AutoEmbedField.EmbedField}.
   * Explicitly declared non-auto-embed fields become passthroughs at their full dotted path; a
   * nested {@code document} with dynamic mapping enabled becomes a single wholesale passthrough at
   * its path with its auto-embed declarations still collected. When the source index has dynamic
   * mapping enabled at the root, passthrough is additionally inferred at read time for every
   * non-embed path so the materialized view carries every source field.
   */
  public static SearchIndexAutoEmbedFieldMapping createAutoEmbedMapping(
      SearchIndexDefinition searchDef) {
    ImmutableMap.Builder<FieldPath, AutoEmbedField.EmbedField> embedBuilder =
        ImmutableMap.builder();
    ImmutableSet.Builder<FieldPath> passthroughBuilder = ImmutableSet.builder();
    collectSearchAutoEmbedFields(
        searchDef.getMappings().fields(),
        Optional.empty(),
        autoEmbedDef -> {
          FieldPath source = autoEmbedDef.sourceField();
          embedBuilder.put(
              source, new AutoEmbedField.EmbedField(source, autoEmbedDef.specification()));
        },
        passthroughBuilder);
    return new SearchIndexAutoEmbedFieldMapping(
        searchDef.getMappings().dynamic(),
        embedBuilder.build(),
        passthroughBuilder.build(),
        searchMappingIsStaticallyEnumerable(searchDef));
  }

  /**
   * Walks the search-index field hierarchy, handing each auto-embed declaration to {@code
   * onAutoEmbed} and collecting passthrough destination paths. A field's type alternatives are
   * processed independently (search mappings may declare several types per field). Static {@code
   * document} alternatives that contain an auto-embed descendant are descended into so the nested
   * auto-embed field is embedded rather than copied wholesale; auto-embed-free subtrees are passed
   * through as a whole. The whole field is passed through at its own path when its dynamic mapping
   * or any other type alternative needs the raw source value in the materialized view: a dynamic
   * {@code document}'s undeclared children and a scalar alternative's value must both reach the MV.
   * The embed-path carve-out in {@code isPassthrough} keeps embed sources out of wholesale copies:
   * an auto-embed source field is consumed by the embedding pipeline and is never copied to the MV
   * — and therefore never lexically indexed — in every mode, dynamic mappings included. Explicit
   * declarations overlapping a source field are rejected at validation; dynamic coverage of a
   * source path silently yields no lexical data for it by design. {@code embeddedDocuments} fields
   * cannot contain auto-embed fields (rejected at validation), so they are passed through as a
   * whole.
   */
  private static void collectSearchAutoEmbedFields(
      Map<String, FieldDefinition> fields,
      Optional<FieldPath> prefix,
      Consumer<SearchAutoEmbedFieldDefinition> onAutoEmbed,
      ImmutableSet.Builder<FieldPath> passthroughBuilder) {
    for (Map.Entry<String, FieldDefinition> entry : fields.entrySet()) {
      FieldPath path =
          prefix
              .map(p -> p.newChild(entry.getKey()))
              .orElseGet(() -> FieldPath.newRoot(entry.getKey()));
      FieldDefinition fieldDef = entry.getValue();
      var autoEmbed = fieldDef.searchAutoEmbedFieldDefinition();
      var document = fieldDef.documentFieldDefinition();
      autoEmbed.ifPresent(onAutoEmbed);
      boolean documentHasEmbeds =
          document.isPresent() && document.get().containsSearchAutoEmbedField();
      boolean hasOtherTypes =
          fieldDef.hasFieldDefinitionsOtherThan(
              FieldTypeDefinition.Type.AUTO_EMBED, FieldTypeDefinition.Type.DOCUMENT);
      if (documentHasEmbeds) {
        if (document.get().dynamic().isEnabled() || hasOtherTypes) {
          passthroughBuilder.add(path);
          collectAutoEmbedFieldsOnly(document.get().fields(), onAutoEmbed);
        } else {
          collectSearchAutoEmbedFields(
              document.get().fields(), Optional.of(path), onAutoEmbed, passthroughBuilder);
        }
      } else if (hasOtherTypes || document.isPresent() || autoEmbed.isEmpty()) {
        passthroughBuilder.add(path);
      }
    }
  }

  /**
   * Collects auto-embed declarations from a subtree whose non-embed content is passed through
   * wholesale (a dynamic {@code document} or one shadowed by another type alternative), without
   * enumerating per-field passthrough paths.
   */
  private static void collectAutoEmbedFieldsOnly(
      Map<String, FieldDefinition> fields, Consumer<SearchAutoEmbedFieldDefinition> onAutoEmbed) {
    for (FieldDefinition fieldDef : fields.values()) {
      fieldDef.searchAutoEmbedFieldDefinition().ifPresent(onAutoEmbed);
      fieldDef
          .documentFieldDefinition()
          .ifPresent(document -> collectAutoEmbedFieldsOnly(document.fields(), onAutoEmbed));
    }
  }

  /**
   * Creates an {@code AutoEmbedFieldMapping} describing the materialized-view side of an auto-embed
   * index, dispatching to the index-type-specific factory.
   */
  public static AutoEmbedFieldMapping createMatViewAutoEmbedMapping(
      IndexDefinition indexDefinition, MaterializedViewSchemaMetadata schemaMetadata) {
    return switch (indexDefinition) {
      case VectorIndexDefinition v -> createMatViewAutoEmbedMapping(v, schemaMetadata);
      case SearchIndexDefinition s -> createMatViewAutoEmbedMapping(s, schemaMetadata);
    };
  }

  /**
   * Creates a new AutoEmbedFieldMapping from a VectorIndexDefinition by converting AUTO_EMBED
   * fields and adding hash fields for a materialized view.
   *
   * <p>For example: {'title': {'path': 'title', 'type': 'autoEmbed', 'model': 'voyage-4',
   * 'modality': 'text'}, 'plot': {'path': 'plot', 'type': 'filter'}}
   *
   * <p>will be converted to:
   *
   * <p>{'_autoEmbed.title': {'path': '_autoEmbed.title', 'type': 'autoEmb', 'numDimensions': 1024,
   * 'similarity': 'dotProduct'}, '_autoEmbed._hash.title': {'path': '_autoEmbed._hash.title',
   * 'type': 'filter'}, 'plot': {'path': 'plot', 'type': 'filter'}}
   *
   * <p>AUTO_EMBED field paths are remapped according to the schema metadata. FILTER and TEXT fields
   * are preserved as-is. VECTOR and EMBEDDED_DOCUMENTS fields are omitted. For each AUTO_EMBED
   * field, a corresponding hash field is added as a passthrough field.
   */
  public static AutoEmbedFieldMapping createMatViewAutoEmbedMapping(
      VectorIndexDefinition indexDefinition, MaterializedViewSchemaMetadata schemaMetadata) {
    VectorIndexFieldMapping mappings = indexDefinition.getMappings();
    Optional<FieldPath> sourceNestedRoot = indexDefinition.getNestedRoot();
    ImmutableMap.Builder<FieldPath, AutoEmbedField> builder = ImmutableMap.builder();
    for (Map.Entry<FieldPath, VectorIndexFieldDefinition> entry : mappings.fieldMap().entrySet()) {
      FieldPath path = entry.getKey();
      VectorIndexFieldDefinition field = entry.getValue();
      switch (field.getType()) {
        case AUTO_EMBED -> {
          var spec = field.asVectorAutoEmbedField().specification();
          FieldPath matViewPath =
              getMatViewFieldPath(path, schemaMetadata.autoEmbeddingFieldsMapping());
          builder.put(matViewPath, new AutoEmbedField.EmbedField(matViewPath, spec));
          FieldPath hashPath =
              getHashFieldPath(path, schemaMetadata.materializedViewSchemaVersion());
          builder.put(hashPath, new AutoEmbedField.PassthroughField(hashPath));
        }
        case TEXT -> {
          var spec = field.asVectorTextField().specification();
          builder.put(path, new AutoEmbedField.EmbedField(path, spec));
        }
        case FILTER -> {
          // CLOUDP-402522: filter fields under the source nestedRoot must be remapped into the MV
          // namespace so they live alongside the auto-embedded vector at the embedded child
          // level. Top-level filters keep their source path.
          FieldPath matViewPath =
              isFilterUnderSourceNestedRoot(path, sourceNestedRoot)
                  ? getMatViewFieldPath(path, schemaMetadata.autoEmbeddingFieldsMapping())
                  : path;
          builder.put(matViewPath, new AutoEmbedField.PassthroughField(matViewPath));
        }
        case VECTOR -> {
          // Output field written by the embedding provider, not a source field.
        }
        case EMBEDDED_DOCUMENTS -> {
          // Structural array marker, not a leaf field to process.
        }
      }
    }
    return new VectorAutoEmbedFieldMapping(builder.build());
  }

  /**
   * Creates an {@code AutoEmbedFieldMapping} describing the materialized-view side of a
   * search-backed auto-embed index. Auto-embed source paths (declared at any {@code document}
   * nesting depth) are rewritten to their MV namespace via {@code
   * schemaMetadata.autoEmbeddingFieldsMapping()}; a passthrough hash path is added per embed field
   * for staleness detection. Declared non-auto-embed fields stay at their original (possibly
   * dotted) path. The source mapping's {@code dynamic} flag is preserved so dynamic-mode MVs treat
   * every non-embed path as passthrough.
   */
  public static AutoEmbedFieldMapping createMatViewAutoEmbedMapping(
      SearchIndexDefinition searchDef, MaterializedViewSchemaMetadata schemaMetadata) {
    ImmutableMap.Builder<FieldPath, AutoEmbedField.EmbedField> embedBuilder =
        ImmutableMap.builder();
    ImmutableSet.Builder<FieldPath> passthroughBuilder = ImmutableSet.builder();
    collectSearchAutoEmbedFields(
        searchDef.getMappings().fields(),
        Optional.empty(),
        autoEmbedDef -> {
          FieldPath sourcePath = autoEmbedDef.sourceField();
          FieldPath matViewPath =
              getMatViewFieldPath(sourcePath, schemaMetadata.autoEmbeddingFieldsMapping());
          embedBuilder.put(
              matViewPath,
              new AutoEmbedField.EmbedField(matViewPath, autoEmbedDef.specification()));
          passthroughBuilder.add(
              getHashFieldPath(sourcePath, schemaMetadata.materializedViewSchemaVersion()));
        },
        passthroughBuilder);
    return new SearchIndexAutoEmbedFieldMapping(
        searchDef.getMappings().dynamic(),
        embedBuilder.build(),
        passthroughBuilder.build(),
        searchMappingIsStaticallyEnumerable(searchDef));
  }

  /**
   * Returns whether every path indexed by this Search mapping is statically enumerable. Dynamic
   * indexing at the root or in any document-like subtree makes destructive partial-update safety
   * impossible to prove from the finite field map.
   */
  private static boolean searchMappingIsStaticallyEnumerable(SearchIndexDefinition searchDef) {
    return hierarchyIsStaticallyEnumerable(searchDef.getMappings());
  }

  private static boolean hierarchyIsStaticallyEnumerable(
      HierarchicalFieldDefinition hierarchicalDefinition) {
    return !hierarchicalDefinition.dynamic().isEnabled()
        && hierarchicalDefinition.fields().values().stream()
            .allMatch(
                field ->
                    field
                            .documentFieldDefinition()
                            .map(AutoEmbedFieldMappingCreator::hierarchyIsStaticallyEnumerable)
                            .orElse(true)
                        && field
                            .embeddedDocumentsFieldDefinition()
                            .map(AutoEmbedFieldMappingCreator::hierarchyIsStaticallyEnumerable)
                            .orElse(true));
  }

  /**
   * Returns the hash field path for the given field path by materialized view schema version.
   *
   * <p>For version 0, appends _hash to the leaf. For version 1+, prepends '_autoEmbed._hash.'.
   */
  public static FieldPath getHashFieldPath(FieldPath fieldPath, long matViewSchemaVersion) {
    // TODO(CLOUDP-363914): build hash field from MV schema metadata, not by version.
    if (matViewSchemaVersion == 0) {
      return fieldPath
          .getParent()
          .map(path -> path.newChild(fieldPath.getLeaf() + HASH_FIELD_SUFFIX))
          .orElse(FieldPath.newRoot(fieldPath.getLeaf() + HASH_FIELD_SUFFIX));
    }
    return FieldPath.parse(HASH_FIELD_PREFIX + FieldPath.DELIMITER + fieldPath.toString());
  }

  /**
   * Converts a source auto-embedding field path to the materialized view field path using the
   * schema fields mapping. Returns the original path if no mapping exists.
   */
  public static FieldPath getMatViewFieldPath(
      FieldPath sourceFieldPath, Map<FieldPath, FieldPath> schemaFieldsMapping) {
    if (schemaFieldsMapping.containsKey(sourceFieldPath)) {
      return schemaFieldsMapping.get(sourceFieldPath);
    }
    return sourceFieldPath;
  }

  /**
   * Computes the MV path for a filter field strictly under {@code sourceNestedRoot} by prepending
   * the {@code _autoEmbed.} namespace (mirrors {@code
   * MaterializedViewCollectionResolver.MV_COLLECTION_SCHEMA_NAMESPACE}). This colocates the filter
   * with the auto-embedded vector under the MV nestedRoot so block-join indexing places both on the
   * same Lucene child; otherwise the filter is indexed at the root while the vector lives at the
   * child level, and nested $vectorSearch with such filters silently matches nothing
   * (CLOUDP-402522).
   */
  public static FieldPath getMatViewFilterPathUnderNestedRoot(
      FieldPath sourceFilterPath, FieldPath sourceNestedRoot) {
    Check.checkArg(
        sourceFilterPath.isChildOf(sourceNestedRoot),
        "sourceFilterPath %s must be a strict descendant of sourceNestedRoot %s",
        sourceFilterPath,
        sourceNestedRoot);
    return FieldPath.parse(MV_NAMESPACE_PREFIX + sourceFilterPath.toString());
  }

  /**
   * Returns true when {@code sourceFilterPath} is a strict descendant of {@code sourceNestedRoot}
   * (when present). Filter fields under the nestedRoot must be remapped into the MV namespace so
   * that they live at the embedded child level alongside the auto-embedded vector.
   */
  public static boolean isFilterUnderSourceNestedRoot(
      FieldPath sourceFilterPath, Optional<FieldPath> sourceNestedRoot) {
    return sourceNestedRoot.isPresent() && sourceFilterPath.isChildOf(sourceNestedRoot.get());
  }

  /**
   * Converts a source {@code nestedRoot} to the materialized view {@code nestedRoot} by mirroring
   * the path rewrite applied to auto-embed fields that live under it. Returns the original {@code
   * nestedRoot} if the {@code schemaFieldsMapping} is empty or contains no entry whose source path
   * is a descendant of {@code nestedRoot}.
   *
   * <p>The auto-embed field-path remapping (see {@link
   * com.xgen.mongot.embedding.mongodb.MaterializedViewCollectionResolver}) prepends the MV
   * namespace (e.g. {@code _autoEmbed.}) to every source auto-embed path. Because {@code
   * nestedRoot} must be an ancestor of those source paths, the MV-equivalent {@code nestedRoot} is
   * the derived path with the per-entry "tail below the source {@code nestedRoot}" stripped off.
   *
   * <p>Example: with source {@code nestedRoot = "sections"} and mapping {@code
   * "sections.section_content" -> "_autoEmbed.sections.section_content"}, the derived {@code
   * nestedRoot} is {@code "_autoEmbed.sections"}.
   *
   * <p><b>Invariant:</b> all auto-embed fields under the same source {@code nestedRoot} are
   * expected to share the same MV-prefix, and therefore produce the same derived root. This method
   * returns the derived root computed from the first matching entry encountered. If later matching
   * entries disagree, a warning is logged and the lexicographically smallest derived root is
   * returned to ensure a deterministic result regardless of map iteration order.
   */
  public static Optional<FieldPath> getMatViewNestedRoot(
      Optional<FieldPath> sourceNestedRoot, Map<FieldPath, FieldPath> schemaFieldsMapping) {
    if (sourceNestedRoot.isEmpty()) {
      return Optional.empty();
    }
    FieldPath source = sourceNestedRoot.get();
    @Var Optional<FieldPath> result = Optional.empty();
    @Var boolean inconsistencyDetected = false;
    for (Map.Entry<FieldPath, FieldPath> entry : schemaFieldsMapping.entrySet()) {
      FieldPath sourcePath = entry.getKey();
      FieldPath derivedPath = entry.getValue();
      if (!sourcePath.isChildOf(source)) {
        continue;
      }
      FieldPath derivedRoot = stripLevelsBelowRoot(source, sourcePath, derivedPath);
      if (result.isEmpty()) {
        result = Optional.of(derivedRoot);
      } else if (!result.get().equals(derivedRoot)) {
        // Invariant violation: different auto-embed fields under the same nestedRoot disagree on
        // the MV-prefix. Pick the lexicographically smallest derived root to guarantee a
        // deterministic result regardless of map iteration order. Log rather than throw so we
        // fail loud in logs but don't break index derivation over a schema inconsistency.
        inconsistencyDetected = true;
        if (derivedRoot.toString().compareTo(result.get().toString()) < 0) {
          result = Optional.of(derivedRoot);
        }
      }
    }
    if (inconsistencyDetected) {
      LOG.warn(
          "Inconsistent derived nestedRoot across auto-embed mapping entries under source"
              + " nestedRoot={}. Using lexicographically smallest derived root: {}.",
          source,
          result.get());
    }
    return result.isPresent() ? result : sourceNestedRoot;
  }

  private static FieldPath stripLevelsBelowRoot(
      FieldPath sourceNestedRoot, FieldPath sourcePath, FieldPath derivedPath) {
    int levelsBelowRoot =
        sourcePath.getPathHierarchy().size() - sourceNestedRoot.getPathHierarchy().size();
    @Var FieldPath derivedRoot = derivedPath;
    for (int i = 0; i < levelsBelowRoot; i++) {
      derivedRoot =
          derivedRoot
              .getParent()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          String.format(
                              "Derived field path has fewer segments than its source path when"
                                  + " deriving materialized-view nestedRoot: source=%s,"
                                  + " derived=%s, sourceNestedRoot=%s",
                              sourcePath, derivedPath, sourceNestedRoot)));
    }
    return derivedRoot;
  }

  private AutoEmbedFieldMappingCreator() {}
}
