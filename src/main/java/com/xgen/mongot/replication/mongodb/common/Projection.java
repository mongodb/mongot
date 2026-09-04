package com.xgen.mongot.replication.mongodb.common;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.mongodb.client.model.Projections;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory methods to build the projection based on Index Definition. In case when the exact
 * structure is unknown for a part of the document, the whole parent field is eagerly included to
 * avoid excessive data filtering. If the structure is unknown for the whole document, returned
 * {@link Optional} will be empty, which means the projection should not be used.
 *
 * <p>Projection does not always provide better performance and might require dynamic toggling based
 * on benchmark results. See the specification for additional details: https://tinyurl.com/y8bfax2k
 *
 * <p>Note that we always include _id field, even though its original value is preserved under the
 * indexId namespace. If the user modified their _id in the view and configured stored source, we
 * want to store the modified value to accurately represent the view in search response with
 * returnStoredSource: true
 */
public class Projection {
  private static final Logger logger = LoggerFactory.getLogger(Projection.class);

  private enum Type {
    /** Find or aggregate command */
    REGULAR_QUERY("", ImmutableSet.of("_id")),
    /** $changeStream aggregate */
    CHANGE_STREAM(
        "fullDocument",
        ImmutableSet.of(
            "fullDocument._id",
            "operationType",
            "resumeToken",
            "ns",
            "documentKey",
            "clusterTime",
            "updateDescription",
            // "to" carries the destination namespace for RENAME events; without it,
            // getDestinationNamespaceDocument() returns null (HELP-93705).
            // Note: "splitEvent" is intentionally omitted — it is added by the
            // $changeStreamSplitLargeEvent stage which runs after $project, so it is
            // never dropped by the projection and does not need to be listed here.
            "to"));

    private final String pathPrefix;
    private final ImmutableSet<String> defaultIncludedPaths;

    Type(String pathPrefix, ImmutableSet<String> defaultIncludedPaths) {
      this.pathPrefix = pathPrefix;
      this.defaultIncludedPaths = defaultIncludedPaths;
    }

    public ImmutableSet<String> getDefaultIncludedPaths(IndexDefinition definition) {
      return ImmutableSet.<String>builder()
          .addAll(this.defaultIncludedPaths)
          // add indexId namespace that is used to stash internally used fields like _id and the
          // deleted flag
          .add(addTypeSpecificPrefixForProject(definition.getIndexId().toString(), this))
          .build();
    }
  }

  private Projection() {}

  public static Optional<Bson> forRegularQuery(IndexDefinition definition) {
    return build(definition, Type.REGULAR_QUERY);
  }

  public static Optional<Bson> forChangeStream(IndexDefinition definition) {
    return build(definition, Type.CHANGE_STREAM);
  }

  private static Optional<Bson> build(IndexDefinition definition, Type type) {
    return IndexDefinitionProjectionBuilder.build(definition, type);
  }

  private static class IndexDefinitionProjectionBuilder {

    static Optional<Bson> build(IndexDefinition definition, Type type) {

      StoredSourceDefinition storedSource = filterIncompatiblePaths(definition.getStoredSource());
      // requesting full document if the root is stored
      if (storedSource.isAllIncluded()) {
        return Optional.empty();
      }

      List<String> indexedPaths;

      switch (definition) {
        case SearchIndexDefinition searchIndexDefinition -> {
          var mappings = searchIndexDefinition.getMappings();

          // requesting full document if the root is dynamically mapped
          if (mappings.dynamic().isEnabled()) {
            return Optional.empty();
          }

          // Copy into a mutable list: lookupIndexedPathsForProject may return an immutable list,
          // and the autoEmbed sourceField loop below mutates indexedPaths.
          indexedPaths = new ArrayList<>(lookupIndexedPathsForProject(mappings, ""));
          // requesting full document if $project filter is not possible for top-level field
          // mappings
          if (indexedPaths.isEmpty()) {
            return Optional.empty();
          }
          // CLOUDP-353553: lookupIndexedPathsForProject projects each autoEmbed field by its
          // declared index field name (e.g. "desc_embedding"), but embedding reads the source text
          // from the field's sourceField (e.g. "description"). Without projecting the sourceField
          // the source text is stripped from the scanned document and no embedding vector is ever
          // generated. getModelNamePerPath() is keyed by sourceField and covers auto-embed fields
          // at any document nesting depth.
          for (FieldPath sourcePath : searchIndexDefinition.getModelNamePerPath().keySet()) {
            indexedPaths.add(sourcePath.toString());
          }
        }
        case VectorIndexDefinition vectorIndexDefinition -> {
          indexedPaths = new ArrayList<>();
          for (var field : vectorIndexDefinition.getFields()) {
            Optional<FieldPath> path = filterInvalidVectorPaths(field.getPath());
            // requesting full document if $project filter is not possible for top-level field
            // mappings
            if (path.isEmpty()) {
              return Optional.empty();
            }
            indexedPaths.add(path.get().toString());
          }
        }
      }

      switch (storedSource.getMode()) {
        case INCLUSION -> {
          // Perform inclusive $project by merging indexed and stored source paths
          var configuredIncludedPaths =
              Stream.of(indexedPaths, storedSource.getDottedPaths())
                  .flatMap(Collection::stream)
                  .map(path -> addTypeSpecificPrefixForProject(path, type))
                  .collect(toList());

          var includes =
              filterPathCollisions(
                      CollectionUtils.concat(
                          configuredIncludedPaths, type.getDefaultIncludedPaths(definition)))
                  .stream()
                  .map(Projections::include)
                  .collect(toList());

          return Optional.of(Projections.fields(includes));
        }
        case EXCLUSION -> {
          Set<FieldPath> pathsToMaybeExclude =
              storedSource.getDottedPaths().stream()
                  .map(FieldPath::parse)
                  .collect(toCollection(HashSet::new)); // Guarantee mutability

          List<FieldPath> mustInclude =
              indexedPaths.stream().map(FieldPath::parse).collect(toList());

          // Do not exclude any path which is an ancestor of a required path.
          pathsToMaybeExclude.removeIf(p -> mustInclude.stream().anyMatch(p::isDirectRelation));

          List<String> realExclusions =
              pathsToMaybeExclude.stream().map(FieldPath::toString).collect(toList());

          var excludes =
              realExclusions.stream()
                  .map(path -> addTypeSpecificPrefixForProject(path, type))
                  .filter(
                      path ->
                          !type.getDefaultIncludedPaths(definition)
                              .contains(path)) // ensures all default fields are included
                  .sorted() // guarantees a deterministic order regardless of the provided input
                  .map(Projections::exclude)
                  .collect(toList());

          if (excludes.isEmpty()) {
            // $project spec must have at least one field
            return Optional.empty();
          }

          return Optional.of(Projections.fields(excludes));
        }
      }
      logger.error("Unhandled projection type: {}", type);
      return Check.unreachable("Unhandled projection type");
    }

    private static Optional<FieldPath> filterInvalidVectorPaths(FieldPath path) {
      for (FieldPath ancestor : path.getPathHierarchy()) {

        // field name is unsupported, so requesting its parent
        if (ancestor.getLeaf().startsWith("$")) {
          return ancestor.getParent();
        }
      }
      return Optional.of(path);
    }

    /**
     * Checks if stored source is enabled and contains any paths which are incompatible with MongoDB
     * $project. If that's the case, simply returns full inclusion to request unmodified documents
     * and be able to process them correctly on our side.
     */
    private static StoredSourceDefinition filterIncompatiblePaths(
        StoredSourceDefinition definition) {
      boolean containsIncompatiblePaths =
          definition.getDottedPaths().stream()
              .anyMatch(path -> path.startsWith(".") || path.endsWith(".") || path.contains(".."));
      return containsIncompatiblePaths ? StoredSourceDefinition.createIncludeAll() : definition;
    }

    /**
     * Returns a list of paths for $project, or an empty list if projection is not possible.
     *
     * <p>The returned list should be the minimal set of paths required to index the given {@link
     * DocumentFieldDefinition}. Note: the returned list does not guarantee unique values. Field
     * names must be deduplicated before constructing a $project stage. <br>
     * Reasons project might not be possible include:
     *
     * <ol>
     *   <li>Field names are unknown (index has top-level dynamic: true)
     *   <li>Top-level contains field that is not (easily) included in a $project stage (name
     *       contains '.' or '$')
     * </ol>
     */
    private static List<String> lookupIndexedPathsForProject(
        DocumentFieldDefinition document, String parentPath) {

      if (document.dynamic().isEnabled()) {
        return includeParentField(parentPath); // include everything under the parent field
      }

      var includes = new ArrayList<String>();

      for (var entry : document.fields().entrySet()) {

        var fieldName = entry.getKey();

        if (fieldName.isEmpty() || fieldName.contains(".") || fieldName.startsWith("$")) {
          // projection query can't work with such fields, but they can appear in the index and
          // data,
          // so we should be able to index them - including the whole parent field to do that
          return includeParentField(parentPath);
        }

        var fieldPath =
            parentPath.isEmpty() ? fieldName : parentPath + FieldPath.DELIMITER + fieldName;
        var fieldDefinition = entry.getValue();

        if (fieldDefinition.documentFieldDefinition().isPresent()) {
          if (fieldDefinition.hasScalarFieldDefinitions()) {
            // project current field and everything under, since its mapping
            // contains both scalar and document definitions
            includes.add(fieldPath);
          } else {
            // recursively project a nested document
            includes.addAll(
                lookupIndexedPathsForProject(
                    fieldDefinition.documentFieldDefinition().get(), fieldPath));
          }
        } else {
          // project a regular field
          includes.add(fieldPath);
        }
      }

      return includes;
    }

    /** Returns a parent field name, or an empty list if the parentPath represents a root. */
    private static List<String> includeParentField(String parentPath) {
      return parentPath.isEmpty() ? List.of() : List.of(parentPath);
    }
  }

  /** Returns a set of paths such that no element is ancestor or duplicate of another. */
  private static Set<String> filterPathCollisions(ImmutableList<String> pathsWithCollisions) {
    var result = new LinkedHashSet<String>();

    pathsWithCollisions.stream()
        .distinct() // removes duplicates
        .sorted() // guarantees broader paths to go first
        .map(FieldPath::parse)
        .forEach(
            path -> {
              var collision =
                  path.ancestorPaths().anyMatch(ancestor -> result.contains(ancestor.toString()));
              if (!collision) {
                result.add(path.toString());
              }
            });

    return result;
  }

  private static String addTypeSpecificPrefixForProject(String path, Type type) {
    return type.pathPrefix.isEmpty() ? path : type.pathPrefix + FieldPath.DELIMITER + path;
  }
}
