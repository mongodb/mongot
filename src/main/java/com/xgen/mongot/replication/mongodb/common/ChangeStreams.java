package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.TruncatedArray;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.definition.FieldDefinitionResolver;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.FieldPath;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

public class ChangeStreams {

  private static final Pattern ONLY_DIGITS = Pattern.compile("\\d+");

  /**
   * Returns a boolean indicating whether the supplied UpdateDescription contains any fields which
   * are indexed by the supplied IndexDefinition.
   */
  public static boolean updateDescriptionAppliesToIndex(
      UpdateDescription updateDescription,
      IndexDefinition indexDefinition,
      FieldDefinitionResolver fieldDefinitionResolver,
      Optional<ViewDefinition> view) {

    /*
     * If the definition is configured on a view, we can't rely on update description to discard the
     * event. E.g. if a source of synthetic fields added via $addFields is not participating in the
     * mappings, we still need to process the event as its updateDescription won't contain data
     * about the synthetic fields themselves. Similarly, we can have a situation when a previously
     * deleted document got updated so that it now satisfies $match stage - while updateDescription
     * might not contain fields used in $match, we should still make sure the document is
     * re-inserted.
     */
    if (view.isPresent()) {
      return true;
    }

    List<String> removedFields =
        Optional.ofNullable(updateDescription.getRemovedFields()).orElseGet(Collections::emptyList);

    Set<String> updatedFields =
        Optional.ofNullable(updateDescription.getUpdatedFields())
            .map(BsonDocument::keySet)
            .orElseGet(Collections::emptySet);

    Set<String> truncatedArrays =
        Optional.ofNullable(updateDescription.getTruncatedArrays())
            .map(
                arrays -> arrays.stream().map(TruncatedArray::getField).collect(Collectors.toSet()))
            .orElseGet(Collections::emptySet);

    ImmutableList<String> fields =
        CollectionUtils.concat(removedFields, updatedFields, truncatedArrays);

    // Convert all the fields in the UpdateDescription to the field we need to check for being
    // configured to be indexed or stored, and return true if any of the fields are configured.
    return fields.stream()
        .map(FieldPath::parse)
        .map(ChangeStreams::filterFieldPath)
        .anyMatch(
            fieldPath ->
                fieldDefinitionResolver.isUsed(fieldPath)
                    || isSearchAutoEmbedPath(indexDefinition, fieldPath));
  }

  private static boolean isSearchAutoEmbedPath(
      IndexDefinition indexDefinition, FieldPath updatedFieldPath) {
    if (!(indexDefinition instanceof SearchIndexDefinition searchIndexDefinition)) {
      return false;
    }

    return searchIndexDefinition.getModelNamePerPath().keySet().stream()
        .anyMatch(updatedFieldPath::isDirectRelation);
  }

  /**
   * Converts the given FieldPath that is present in the UpdateDescription into the path that should
   * be checked in the IndexDefinition.
   *
   * <p>Simply checking for the exact field path does not work in the presence of arrays. When a
   * field referenced in updatedFields or removedFields has an integer as part of its path (e.g.
   * "a.b.0") we cannot tell if that integer is an index into an array, or simply the name of a
   * field (e.g. { a: { b: { 0: 'my value' } } }).
   *
   * <p>When we receive an update description that references "a.b.0.c" we are unable to precisely
   * say what the schema of the updated document was. The value of "a.b" may be an array or a
   * document. If "a.b" is an array, we should be checking to see if the field "a.b.c" is indexed.
   * If "a.b" is a document, we should be checking to see if the field "a.b.0.c" is indexed.
   *
   * <p>One option for handling this case would be to check for both "a.b.c" and "a.b.0.c", and if
   * either of them is indexed to not filter the event. However, determining the fields we have to
   * check gets complicated when considering arrays of objects with arrays of objects (or even
   * deeper nestings). For "a.b.0.c.1.d" we would need to check "a.b.c.d", "a.b.0.c.d", "a.b.c.1.d",
   * and "a.b.0.c.1.d".
   *
   * <p>Instead of trying to determine exactly which fields need to be checked, we can simplify the
   * problem by considering any path that has an integer child as having been updated completely.
   * For example, if we get the path "a.b.0.c", instead of trying to check for "a.b.c" or "a.b.0.c",
   * simply consider all of "a.b" to have changed, and check if "a.b" is indexed.
   *
   * <p>Note that this relies on the fact that if a field in an embedded document is indexed, all of
   * its ancestors up to the root must also be configured to be indexed. For example, if "a.b.c" is
   * to be indexed as a string, "a.b" and "a" must also be configured to at least be indexed as a
   * document. Additionally, any field whose first matching ancestor is defined as dynamic will be
   * returned with a value from IndexDefinition::getField. That is, if the root mapping is dynamic,
   * "a.b.c" would return as configured to be indexed.
   */
  static FieldPath filterFieldPath(FieldPath fieldPath) {
    // Get the path elements up to but not including the first integer path element.
    ImmutableList<FieldPath> integerPrefix =
        fieldPath.getPathHierarchy().stream()
            .takeWhile(p -> !ONLY_DIGITS.matcher(p.getLeaf()).matches())
            .collect(ImmutableList.toImmutableList());

    // If there was no prefix, meaning the first field path element was an integer, return that
    // first field path element since it cannot be an array index since the root must be a document.
    if (integerPrefix.size() == 0) {
      return fieldPath.getPathHierarchy().get(0);
    }

    // If the prefix was the same length as the original field path, that means there were no
    // integer elements and we can simply return the original field path.
    if (integerPrefix.size() == fieldPath.getPathHierarchy().size()) {
      return fieldPath;
    }

    // Otherwise, there was an integer field path, so get the part of the path containing no digits
    return integerPrefix.getLast();
  }

  /**
   * Renaming collection Y to X can atomically drop collection X if dropTarget is set to true (see
   * SERVER-53478).
   *
   * <p>To identify such an event it is sufficient to compare X to destinationNamespace. If
   * dropTarget was false, the rename operation would have been rejected.
   *
   * <p>This may happen as a result of an $out stage too:
   * https://docs.mongodb.com/v4.2/reference/operator/aggregation/out/index.html#replace-existing-collection
   */
  public static boolean renameCausedCollectionDrop(
      ChangeStreamDocument<RawBsonDocument> event, MongoNamespace collection) {
    checkArg(
        event.getOperationType() == OperationType.RENAME,
        "not a rename event: %s",
        event.getOperationType());

    MongoNamespace destinationNamespace =
        Objects.requireNonNull(
            event.getDestinationNamespace(), "RENAME event did not have a destination namespace");

    // this collection is dropped if some other collection was renamed over its namespace:
    return collection.equals(destinationNamespace);
  }
}
