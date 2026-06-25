package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;

import com.mongodb.client.model.changestream.TruncatedArray;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Set;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ViewFieldDependencies}: static extraction of base-collection field dependencies
 * from a view's effective pipeline, and the per-event decision whether a change-stream update on
 * the base collection may affect the view-resolved indexed document.
 */
@RunWith(JUnit4.class)
public class ViewFieldDependenciesTest {

  private static final FieldPath COMBINED = FieldPath.parse("combinedSearchText");
  private static final FieldPath LANG_CODE = FieldPath.parse("langCode");
  private static final FieldPath APPLICATION_NAMES = FieldPath.parse("applicationNames");

  /** The customer view pipeline that motivated this class: $match + computed $set fields. */
  private static List<BsonDocument> customerPipeline() {
    return List.of(
        BsonDocument.parse("{\"$match\": {\"$expr\": {\"$eq\": [\"$isPublished\", true]}}}"),
        BsonDocument.parse(
            "{\"$set\": {"
                + "\"combinedSearchText\": {\"$concat\": ["
                + "  {\"$ifNull\": [\"$title\", \"\"]}, \"\\n\\n\","
                + "  {\"$ifNull\": [\"$description\", \"\"]}, \"\\n\\n\","
                + "  {\"$reduce\": {"
                + "    \"input\": {\"$map\": {\"input\": {\"$ifNull\": [\"$steps\", []]},"
                + "      \"as\": \"s\","
                + "      \"in\": {\"$concat\": [{\"$ifNull\": [\"$$s.title\", \"\"]}, \". \","
                + "        {\"$ifNull\": [\"$$s.description\", \"\"]}]}}},"
                + "    \"initialValue\": \"\","
                + "    \"in\": {\"$cond\": [{\"$eq\": [\"$$value\", \"\"]}, \"$$this\","
                + "      {\"$concat\": [\"$$value\", \"\\n\\n\", \"$$this\"]}]}}}]},"
                + "\"langCode\": {\"$switch\": {\"branches\": ["
                + "  {\"case\": {\"$eq\": [{\"$type\": \"$language\"}, \"object\"]},"
                + "   \"then\": {\"$ifNull\": [\"$language.langCode\", null]}},"
                + "  {\"case\": {\"$eq\": [{\"$type\": \"$language\"}, \"string\"]},"
                + "   \"then\": \"$language\"}], \"default\": null}},"
                + "\"applicationNames\": {\"$setUnion\": ["
                + "  {\"$filter\": {\"input\": {\"$map\": {"
                + "    \"input\": {\"$ifNull\": [\"$applications\", []]},"
                + "    \"as\": \"a\", \"in\": \"$$a.applicationName\"}},"
                + "   \"as\": \"n\", \"cond\": {\"$and\": [{\"$ne\": [\"$$n\", null]},"
                + "     {\"$ne\": [\"$$n\", \"\"]}]}}}, []]}"
                + "}}"));
  }

  @Test
  public void customerPipeline_matchFieldsOnly() {
    // With no embed paths, the triggers are exactly the $match-referenced base fields.
    ViewFieldDependencies deps = ViewFieldDependencies.derive(customerPipeline(), Set.of());

    assertThat(deps.dependsOnEverything()).isFalse();
    assertThat(deps.getTriggerFields()).containsExactly(FieldPath.parse("isPublished"));
  }

  @Test
  public void customerPipeline_embedFeeders() {
    // Triggers = the $match field ∪ every base field feeding a computed embed source.
    // combinedSearchText -> {title, description, steps}; langCode -> {language} (langCode.langCode
    // collapses to its ancestor); applicationNames -> {applications} (loop vars $$a/$$n add
    // nothing, the bound input does).
    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(
            customerPipeline(), Set.of(COMBINED, LANG_CODE, APPLICATION_NAMES));

    assertThat(deps.getTriggerFields())
        .containsExactly(
            FieldPath.parse("isPublished"),
            FieldPath.parse("title"),
            FieldPath.parse("description"),
            FieldPath.parse("steps"),
            FieldPath.parse("language"),
            FieldPath.parse("applications"));
  }

  @Test
  public void passthroughEmbedPath_feedsFromItself() {
    List<BsonDocument> pipeline =
        List.of(BsonDocument.parse("{\"$match\": {\"$expr\": {\"$eq\": [\"$active\", true]}}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("title")));

    // The $match field plus the passthrough embed path, which feeds from itself.
    assertThat(deps.getTriggerFields())
        .containsExactly(FieldPath.parse("active"), FieldPath.parse("title"));
  }

  @Test
  public void transitiveSetChain_resolvesThroughEarlierStages() {
    List<BsonDocument> pipeline =
        List.of(
            BsonDocument.parse("{\"$set\": {\"a\": \"$x\"}}"),
            BsonDocument.parse("{\"$set\": {\"b\": {\"$concat\": [\"$a\", \"$y\"]}}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("b")));

    assertThat(deps.getTriggerFields()).containsExactly(FieldPath.parse("x"), FieldPath.parse("y"));
  }

  @Test
  public void redefinition_laterStageReplacesFeeders() {
    List<BsonDocument> pipeline =
        List.of(
            BsonDocument.parse("{\"$set\": {\"a\": \"$x\"}}"),
            BsonDocument.parse("{\"$set\": {\"a\": \"$z\"}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("a")));

    assertThat(deps.getTriggerFields()).containsExactly(FieldPath.parse("z"));
  }

  @Test
  public void currentAndRootPrefixes_resolveToBaseFields() {
    List<BsonDocument> pipeline =
        List.of(
            BsonDocument.parse(
                "{\"$set\": {\"c\": {\"$concat\": [\"$$CURRENT.title\", \"$$ROOT.summary\"]}}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("c")));

    assertThat(deps.dependsOnEverything()).isFalse();
    assertThat(deps.getTriggerFields())
        .containsExactly(FieldPath.parse("title"), FieldPath.parse("summary"));
  }

  @Test
  public void bareRootReference_dependsOnEverything() {
    List<BsonDocument> pipeline = List.of(BsonDocument.parse("{\"$set\": {\"copy\": \"$$ROOT\"}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("copy")));

    assertThat(deps.dependsOnEverything()).isTrue();
    assertThat(
            deps.requiresFullProcessing(
                new UpdateDescription(null, new BsonDocument("tags", new BsonInt32(1)))))
        .isTrue();
  }

  @Test
  public void literalSubtree_contributesNoFeeders() {
    List<BsonDocument> pipeline =
        List.of(BsonDocument.parse("{\"$set\": {\"c\": {\"$literal\": \"$title\"}}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("c")));

    assertThat(deps.dependsOnEverything()).isFalse();
    assertThat(deps.getTriggerFields()).isEmpty();
    // The risky direction: a change to the literal's text must NOT force full processing.
    assertThat(deps.requiresFullProcessing(updated("title"))).isFalse();
  }

  @Test
  public void sameStageReferences_resolveAgainstStageInput() {
    // All fields of one $set stage evaluate against the stage INPUT document, so `b` reads the
    // base collection's `a`, not the `a` defined in the same stage.
    List<BsonDocument> pipeline =
        List.of(BsonDocument.parse("{\"$set\": {\"a\": \"$x\", \"b\": \"$a\"}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("b")));

    assertThat(deps.getTriggerFields()).containsExactly(FieldPath.parse("a"));
  }

  @Test
  public void parentRedefinition_invalidatesComputedDescendant() {
    // Redefining `a` wholesale replaces the earlier computed `a.b`; the view-resolved `a.b` is
    // now a subfield of `$y`.
    List<BsonDocument> pipeline =
        List.of(
            BsonDocument.parse("{\"$set\": {\"a.b\": \"$x\"}}"),
            BsonDocument.parse("{\"$set\": {\"a\": \"$y\"}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("a.b")));

    assertThat(deps.getTriggerFields()).containsExactly(FieldPath.parse("y"));
  }

  @Test
  public void sameStageParentAndChild_keepsBothFeeders() {
    // One $set defines both `a` and its sub-path `a.b`. Both feeders must survive regardless of
    // map iteration order — the parent's descendant-invalidation must not drop a same-stage child.
    List<BsonDocument> pipeline =
        List.of(BsonDocument.parse("{\"$set\": {\"a\": \"$x\", \"a.b\": \"$y\"}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("a")));

    assertThat(deps.getTriggerFields()).containsExactly(FieldPath.parse("x"), FieldPath.parse("y"));
  }

  @Test
  public void nestedRefinement_contributesToAncestorFeeders() {
    // A later $set on `a.b` refines the computed `a`, so the view-resolved `a` depends on both.
    List<BsonDocument> pipeline =
        List.of(
            BsonDocument.parse("{\"$set\": {\"a\": \"$y\"}}"),
            BsonDocument.parse("{\"$set\": {\"a.b\": \"$x\"}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("a")));

    assertThat(deps.getTriggerFields()).containsExactly(FieldPath.parse("x"), FieldPath.parse("y"));
  }

  @Test
  public void fieldNameOperators_dependOnEverything() {
    // $getField/$setField carry field names as plain strings the collector cannot interpret.
    List<BsonDocument> pipeline =
        List.of(BsonDocument.parse("{\"$set\": {\"c\": {\"$getField\": \"title\"}}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("c")));

    assertThat(deps.dependsOnEverything()).isTrue();
  }

  @Test
  public void multipleMatchStages_union() {
    List<BsonDocument> pipeline =
        List.of(
            BsonDocument.parse("{\"$match\": {\"$expr\": {\"$eq\": [\"$isPublished\", true]}}}"),
            BsonDocument.parse("{\"$match\": {\"$expr\": {\"$gt\": [\"$rating\", 3]}}}"));

    ViewFieldDependencies deps = ViewFieldDependencies.derive(pipeline, Set.of());

    assertThat(deps.getTriggerFields())
        .containsExactly(FieldPath.parse("isPublished"), FieldPath.parse("rating"));
  }

  @Test
  public void matchOnComputedField_resolvesToItsFeeders() {
    List<BsonDocument> pipeline =
        List.of(
            BsonDocument.parse("{\"$set\": {\"flag\": {\"$gt\": [\"$count\", 5]}}}"),
            BsonDocument.parse("{\"$match\": {\"$expr\": {\"$eq\": [\"$flag\", true]}}}"));

    ViewFieldDependencies deps = ViewFieldDependencies.derive(pipeline, Set.of());

    assertThat(deps.getTriggerFields()).containsExactly(FieldPath.parse("count"));
  }

  @Test
  public void computedFilterField_feedersAreTriggers_passthroughFilterIsNot() {
    // A view-computed filter field's feeders must be triggers (the partial $set path cannot delete
    // a filter field the view drops); a passthrough filter field must NOT be a trigger (else the
    // filter-only optimization is defeated for the common case).
    List<BsonDocument> pipeline =
        List.of(
            BsonDocument.parse(
                "{\"$set\": {\"flag\": {\"$cond\": [{\"$gt\": [\"$score\", 5]},"
                    + " \"$$REMOVE\", \"low\"]}}}"));

    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(
            pipeline, Set.of(), Set.of(FieldPath.parse("flag"), FieldPath.parse("category")));

    // `flag` is computed -> its feeder `score` is a trigger; `category` is passthrough -> absent.
    assertThat(deps.getTriggerFields()).containsExactly(FieldPath.parse("score"));
  }

  @Test
  public void unsupportedStage_dependsOnEverything() {
    List<BsonDocument> pipeline = List.of(BsonDocument.parse("{\"$project\": {\"x\": 1}}"));

    ViewFieldDependencies deps = ViewFieldDependencies.derive(pipeline, Set.of());

    assertThat(deps.dependsOnEverything()).isTrue();
  }

  // ---------------------------------------------------------------------------------------------
  // Per-event decision
  // ---------------------------------------------------------------------------------------------

  private static ViewFieldDependencies customerDeps() {
    return ViewFieldDependencies.derive(customerPipeline(), Set.of(COMBINED));
  }

  private static UpdateDescription updated(String... fields) {
    BsonDocument updates = new BsonDocument();
    for (String field : fields) {
      updates.append(field, new BsonString("v"));
    }
    return new UpdateDescription(null, updates);
  }

  @Test
  public void decision_nullOrEmptyUpdateDescription_requiresFull() {
    assertThat(customerDeps().requiresFullProcessing(null)).isTrue();
    assertThat(customerDeps().requiresFullProcessing(new UpdateDescription(null, null))).isTrue();
    assertThat(customerDeps().requiresFullProcessing(new UpdateDescription(List.of(), null)))
        .isTrue();
  }

  @Test
  public void decision_matchFieldChanged_requiresFull() {
    assertThat(customerDeps().requiresFullProcessing(updated("isPublished"))).isTrue();
  }

  @Test
  public void decision_embedFeederChanged_requiresFull() {
    assertThat(customerDeps().requiresFullProcessing(updated("title"))).isTrue();
    assertThat(customerDeps().requiresFullProcessing(updated("description"))).isTrue();
  }

  @Test
  public void decision_numericArraySegments_normalizedToFeederHit() {
    // Load-bearing for the filterFieldPath normalization: the trigger is a NESTED path `a.b`, and
    // the changed path `a.0.b` only intersects it AFTER the leading numeric segment is stripped to
    // `a` (an ancestor of `a.b`). Without normalization `a.0.b` and `a.b` are unrelated and this
    // would wrongly classify as filter-only.
    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(
            List.of(BsonDocument.parse("{\"$set\": {\"c\": \"$a.b\"}}")),
            Set.of(FieldPath.parse("c")));
    assertThat(deps.getTriggerFields()).containsExactly(FieldPath.parse("a.b"));
    assertThat(deps.requiresFullProcessing(updated("a.0.b"))).isTrue();
  }

  @Test
  public void decision_changedAncestorOfFeeder_requiresFull() {
    List<BsonDocument> pipeline =
        List.of(BsonDocument.parse("{\"$set\": {\"name\": \"$uploadedBy.displayName\"}}"));
    ViewFieldDependencies deps =
        ViewFieldDependencies.derive(pipeline, Set.of(FieldPath.parse("name")));

    // A whole-document replacement of `uploadedBy` may change `uploadedBy.displayName`.
    assertThat(deps.requiresFullProcessing(updated("uploadedBy"))).isTrue();
  }

  @Test
  public void decision_removedFeederField_requiresFull() {
    assertThat(customerDeps().requiresFullProcessing(new UpdateDescription(List.of("title"), null)))
        .isTrue();
  }

  @Test
  public void decision_anyRemovedField_requiresFull() {
    // Removals cannot be applied through the $set-only partial-update path (the removed field
    // is absent from the full document, so its stale value would survive in the materialized
    // view); any removal takes the full-replacement path.
    assertThat(customerDeps().requiresFullProcessing(new UpdateDescription(List.of("tags"), null)))
        .isTrue();
  }

  @Test
  public void decision_truncatedFeederArray_requiresFull() {
    UpdateDescription updateDescription =
        new UpdateDescription(
            null,
            new BsonDocument("tags", new BsonString("v")),
            List.of(new TruncatedArray("steps", 1)));
    assertThat(customerDeps().requiresFullProcessing(updateDescription)).isTrue();
  }

  @Test
  public void decision_unrelatedFieldChanged_filterOnlyRetained() {
    assertThat(customerDeps().requiresFullProcessing(updated("tags"))).isFalse();
    assertThat(customerDeps().requiresFullProcessing(updated("visibility", "uploadedAt")))
        .isFalse();
  }
}
