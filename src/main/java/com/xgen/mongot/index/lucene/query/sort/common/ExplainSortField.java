package com.xgen.mongot.index.lucene.query.sort.common;

import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;

public class ExplainSortField extends SortField {
  private final SortField sortField;
  private final SortFeatureExplainer explainer;

  public ExplainSortField(SortField sortField, SortFeatureExplainer explainer) {
    super(sortField.getField(), sortField.getType(), sortField.getReverse());
    this.sortField = sortField;
    this.explainer = explainer;
  }

  @Override
  public Object getMissingValue() {
    return this.sortField.getMissingValue();
  }

  @Override
  public void setMissingValue(Object missingValue) {
    this.sortField.setMissingValue(missingValue);
  }

  @Override
  public SortField rewrite(IndexSearcher searcher) throws IOException {
    return this.sortField.rewrite(searcher);
  }

  @Override
  public boolean needsScores() {
    return this.sortField.needsScores();
  }

  @Override
  public String getField() {
    return this.sortField.getField();
  }

  @Override
  public Type getType() {
    return this.sortField.getType();
  }

  @Override
  public boolean getReverse() {
    return this.sortField.getReverse();
  }

  @Override
  public FieldComparatorSource getComparatorSource() {
    return this.sortField.getComparatorSource();
  }

  @Override
  public String toString() {
    return "<explain_field: " + this.sortField + "\">";
  }

  @Override
  public IndexSorter getIndexSorter() {
    return this.sortField.getIndexSorter();
  }

  @Override
  public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
    var sortFieldComparator = this.sortField.getComparator(numHits, pruning);

    return new FieldComparatorExplainWrapper<>(sortFieldComparator, this.explainer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof ExplainSortField that)) {
      // Handle symmetric equality between SortField and ExplainSortField
      // This is required to ensure index sort equality checks succeed when determining if
      // a query sort can benefit from index sort during TopFieldCollector's docs collection.
      // Without this, wrapped ExplainSortField instances wouldn't match their underlying
      // SortField counterparts, preventing early termination optimization.
      if (o instanceof SortField that) {
        return Objects.equals(this.sortField, that);
      }
      return false;
    }

    if (!super.equals(o)) {
      return false;
    }

    return Objects.equals(this.sortField, that.sortField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), this.sortField);
  }

  public SortField getSortField() {
    return this.sortField;
  }
}
