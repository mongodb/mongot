package com.xgen.testing.mongot.index.query.highlights;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.highlights.Highlight;
import com.xgen.mongot.index.query.highlights.UnresolvedHighlight;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HighlightBuilder {
  private final List<StringPath> paths = new ArrayList<>();
  private final List<StringPath> storedPaths = new ArrayList<>();
  private Optional<Integer> maxNumPassages = Optional.empty();
  private Optional<Integer> maxCharsToExamine = Optional.empty();
  private Optional<FieldPath> returnScope = Optional.empty();

  public static HighlightBuilder builder() {
    return new HighlightBuilder();
  }

  public HighlightBuilder path(String path) {
    this.paths.add(StringPathBuilder.fieldPath(path));
    return this;
  }

  public HighlightBuilder multi(String path, String multi) {
    this.paths.add(StringPathBuilder.withMulti(path, multi));
    return this;
  }

  public HighlightBuilder storedPath(String path) {
    this.storedPaths.add(StringPathBuilder.fieldPath(path));
    return this;
  }

  public HighlightBuilder storedMulti(String path, String multi) {
    this.storedPaths.add(StringPathBuilder.withMulti(path, multi));
    return this;
  }

  public HighlightBuilder maxNumPassages(int maxNumPassages) {
    this.maxNumPassages = Optional.of(maxNumPassages);
    return this;
  }

  public HighlightBuilder maxCharsToExamine(int maxCharsToExamine) {
    this.maxCharsToExamine = Optional.of(maxCharsToExamine);
    return this;
  }

  public HighlightBuilder returnScope(FieldPath returnScope) {
    this.returnScope = Optional.of(returnScope);
    return this;
  }

  /** Build the Highlight. */
  public Highlight build() {
    if (this.paths.size() != this.storedPaths.size()) {
      throw new IllegalStateException(
          String.format(
              "Mismatch between paths size (%d) and storedPaths size (%d)",
              this.paths.size(), this.storedPaths.size()));
    }

    Map<String, String> storedLuceneFieldNameMap = new HashMap<>();
    for (int i = 0; i < this.paths.size(); i++) {
      String lucenePath =
          FieldName.getLuceneFieldNameForStringPath(this.paths.get(i), this.returnScope);
      String storedPath =
          FieldName.getLuceneFieldNameForStringPath(this.storedPaths.get(i), this.returnScope);
      storedLuceneFieldNameMap.put(lucenePath, storedPath);
    }
    return Highlight.create(
        storedLuceneFieldNameMap,
        this.maxNumPassages.orElse(UnresolvedHighlight.Fields.MAX_NUM_PASSAGES.getDefaultValue()),
        this.maxCharsToExamine.orElse(
            UnresolvedHighlight.Fields.MAX_CHARS_TO_EXAMINE.getDefaultValue()));
  }
}
