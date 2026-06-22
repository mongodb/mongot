package com.xgen.mongot.index.lucene.query.sort.common;

import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.profiler.ProfileDocIdSetIterator;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.jetbrains.annotations.Nullable;

public class FieldComparatorExplainWrapper<T> extends FieldComparator<T> {
  private final FieldComparator<T> wrappedComparator;
  private final SortFeatureExplainer featureExplainer;

  public FieldComparatorExplainWrapper(
      FieldComparator<T> wrappedComparator, SortFeatureExplainer featureExplainer) {
    this.wrappedComparator = wrappedComparator;
    this.featureExplainer = featureExplainer;
  }

  @Override
  public int compare(int slot1, int slot2) {
    return this.wrappedComparator.compare(slot1, slot2);
  }

  @Override
  public void setTopValue(T value) {
    this.wrappedComparator.setTopValue(value);
  }

  @Override
  public int compareValues(T first, T second) {
    return this.wrappedComparator.compareValues(first, second);
  }

  @Nullable
  @Override
  public T value(int slot) {
    return this.wrappedComparator.value(slot);
  }

  @Override
  public void setSingleSort() {
    this.wrappedComparator.setSingleSort();
  }

  @Override
  public void disableSkipping() {
    this.wrappedComparator.disableSkipping();
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    return new ExplainLeafFieldComparator(
        this.wrappedComparator.getLeafComparator(context), this.featureExplainer);
  }

  private static class ExplainLeafFieldComparator implements LeafFieldComparator {
    private final LeafFieldComparator wrappedLeafComparator;
    private final SortFeatureExplainer featureExplainer;

    ExplainLeafFieldComparator(
        LeafFieldComparator wrappedLeafComparator, SortFeatureExplainer sortFeatureExplainer) {
      this.wrappedLeafComparator = wrappedLeafComparator;
      this.featureExplainer = sortFeatureExplainer;
    }

    @Override
    public void setBottom(int slot) throws IOException {
      try (var ignored = this.featureExplainer.getTimings().split(ExplainTimings.Type.SET_BOTTOM)) {
        this.wrappedLeafComparator.setBottom(slot);
      }
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      try (var ignored =
          this.featureExplainer.getTimings().split(ExplainTimings.Type.COMPARE_BOTTOM)) {
        return this.wrappedLeafComparator.compareBottom(doc);
      }
    }

    @Override
    public int compareTop(int doc) throws IOException {
      try (var ignored =
          this.featureExplainer.getTimings().split(ExplainTimings.Type.COMPARE_TOP)) {
        return this.wrappedLeafComparator.compareTop(doc);
      }
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      this.wrappedLeafComparator.copy(slot, doc);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      try (var ignored = this.featureExplainer.getTimings().split(ExplainTimings.Type.SET_SCORER)) {
        this.wrappedLeafComparator.setScorer(scorer);
      }
    }

    @Override
    public @Nullable DocIdSetIterator competitiveIterator() throws IOException {
      Optional<ProfileDocIdSetIterator> iterator;
      try (var ignored =
          this.featureExplainer.getTimings().split(ExplainTimings.Type.COMPETITIVE_ITERATOR)) {
        // use new explain timings here to be safe
        iterator =
            Optional.ofNullable(this.wrappedLeafComparator.competitiveIterator())
                .map(
                    iter -> ProfileDocIdSetIterator.create(iter, ExplainTimings.builder().build()));
      }

      this.featureExplainer.maybeAddCompetitiveIterator(iterator);

      return iterator.orElse(null);
    }

    @Override
    public void setHitsThresholdReached() throws IOException {
      try (var ignored =
          this.featureExplainer
              .getTimings()
              .split(ExplainTimings.Type.SET_HITS_THRESHOLD_REACHED)) {
        this.wrappedLeafComparator.setHitsThresholdReached();
      }
    }
  }
}
