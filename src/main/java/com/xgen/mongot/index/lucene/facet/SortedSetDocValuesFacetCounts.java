/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xgen.mongot.index.lucene.facet;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;
import org.apache.lucene.facet.FacetUtils;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongValues;
import org.jetbrains.annotations.Nullable;

/**
 * This class is forked entirely from Lucene (9.11) {@link
 * org.apache.lucene.facet.StringValueFacetCounts}. We needed to override the parent class, since
 * the original parent class assumes that the indexed documents appear in a specific format, which
 * it does not for token fields.
 */
public class SortedSetDocValuesFacetCounts extends AbstractSortedSetDocValuesFacetCounts {
  /**
   * Count densely below this many unique values. {@link TokenFacetSparseRouting} uses the same
   * cutoff when deciding whether a global ordinal map is cheap enough to build.
   */
  public static final int DENSE_CARDINALITY_LIMIT = 1024;

  @Nullable int[] denseCounts;
  @Nullable private IntIntHashMap sparseCounts;
  private final int cardinality;

  /** Counts all facet dimensions across the provided hits. */
  public SortedSetDocValuesFacetCounts(SortedSetDocValuesReaderState state, FacetsCollector hits)
      throws IOException {
    super(state);
    this.cardinality = (int) this.dv.getValueCount();
    initializeCounts(hits.getMatchingDocs());
  }

  private void initializeCounts(List<FacetsCollector.MatchingDocs> hits) throws IOException {
    if (this.cardinality < DENSE_CARDINALITY_LIMIT) { // count densely for low cardinality
      this.sparseCounts = null;
      this.denseCounts = null;
      count(hits);
    } else {
      @Var int totalHits = 0;
      @Var int totalDocs = 0;
      for (FacetsCollector.MatchingDocs matchingDocs : hits) {
        totalHits += matchingDocs.totalHits();
        totalDocs += matchingDocs.context().reader().maxDoc();
      }

      // No counting needed if there are no hits:
      if (totalHits == 0) {
        this.sparseCounts = null;
        this.denseCounts = null;
      } else {
        // If our result set is < 10% of the index, we collect sparsely (use hash map). This
        // heuristic is borrowed from IntTaxonomyFacetCounts:
        if (totalHits < totalDocs / 10) {
          this.sparseCounts = new IntIntHashMap();
          this.denseCounts = null;
        } else {
          this.sparseCounts = null;
          this.denseCounts = new int[(int) this.cardinality];
        }
        count(hits);
      }
    }
  }

  @Override
  boolean hasCounts() {
    return this.denseCounts != null || this.sparseCounts != null;
  }

  @Override
  int getCount(int ord) {
    if (this.sparseCounts != null) {
      return this.sparseCounts.get(ord);
    }
    if (this.denseCounts != null) {
      return this.denseCounts[ord];
    }
    return 0;
  }

  @Override
  PrimitiveIterator.OfInt getOrdIterator() {
    if (this.sparseCounts != null) {
      IntIntHashMap map = this.sparseCounts;
      return new PrimitiveIterator.OfInt() {
        final Iterator<IntCursor> iter = map.keys().iterator();

        @Override
        public boolean hasNext() {
          return this.iter.hasNext();
        }

        @Override
        public int nextInt() {
          return this.iter.next().value;
        }
      };
    }
    if (this.denseCounts != null) {
      return IntStream.range(0, this.denseCounts.length).iterator();
    }
    return IntStream.empty().iterator();
  }

  private void countOneSegment(
      @Nullable OrdinalMap ordinalMap,
      LeafReader reader,
      int segOrd,
      @Nullable MatchingDocs hits,
      @Nullable Bits liveDocs)
      throws IOException {
    if (hits != null && hits.totalHits() == 0) {
      return;
    }

    SortedSetDocValues multiValues = DocValues.getSortedSet(reader, this.field);
    if (multiValues == null) {
      // nothing to count
      return;
    }

    if (this.denseCounts == null && this.sparseCounts == null) {
      this.denseCounts = new int[this.cardinality];
    }

    // It's slightly more efficient to work against SortedDocValues if the field is actually
    // single-valued (see: LUCENE-5309)
    SortedDocValues singleValues = DocValues.unwrapSingleton(multiValues);
    DocIdSetIterator valuesIt = singleValues != null ? singleValues : multiValues;

    DocIdSetIterator it;
    if (hits == null) {
      assert liveDocs != null;
      it = FacetUtils.liveDocsDISI(valuesIt, liveDocs);
    } else {
      it = ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits().iterator(), valuesIt));
    }

    if (ordinalMap == null) {
      // If there's no ordinal map we don't need to map segment ordinals to globals, so counting
      // is very straight-forward:
      if (singleValues != null) {
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          increment(singleValues.ordValue());
        }
      } else {
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          for (int i = 0; i < multiValues.docValueCount(); i++) {
            int term = (int) multiValues.nextOrd();
            increment(term);
          }
        }
      }
    } else {
      // We need to map segment ordinals to globals. We have two different approaches to this
      // depending on how many hits we have to count relative to how many unique doc val ordinals
      // there are in this segment:
      LongValues ordMap = ordinalMap.getGlobalOrds(segOrd);
      int segmentCardinality = (int) multiValues.getValueCount();

      if (hits != null && hits.totalHits() < segmentCardinality / 10) {
        // Remap every ord to global ord as we iterate:
        if (singleValues != null) {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            increment((int) ordMap.get(singleValues.ordValue()));
          }
        } else {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            for (int i = 0; i < multiValues.docValueCount(); i++) {
              int term = (int) multiValues.nextOrd();
              increment((int) ordMap.get(term));
            }
          }
        }
      } else {
        // First count in seg-ord space.
        // At this point, we're either counting all ordinals or our heuristic suggests that
        // we expect to visit a large percentage of the unique ordinals (lots of hits relative
        // to the segment cardinality), so we count the segment densely:
        int[] segCounts = new int[segmentCardinality];
        if (singleValues != null) {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            segCounts[singleValues.ordValue()]++;
          }
        } else {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            for (int i = 0; i < multiValues.docValueCount(); i++) {
              int term = (int) multiValues.nextOrd();
              segCounts[term]++;
            }
          }
        }

        // Then, migrate to global ords:
        for (int ord = 0; ord < segmentCardinality; ord++) {
          int count = segCounts[ord];
          if (count != 0) {
            increment((int) ordMap.get(ord), count);
          }
        }
      }
    }
  }

  /** Does all the "real work" of tallying up the counts. */
  private void count(List<MatchingDocs> matchingDocs) throws IOException {

    OrdinalMap ordinalMap;

    if (this.dv instanceof MultiDocValues.MultiSortedSetDocValues && matchingDocs.size() > 1) {
      ordinalMap = ((MultiSortedSetDocValues) this.dv).mapping;
    } else {
      ordinalMap = null;
    }

    IndexReader reader = this.state.getReader();

    for (MatchingDocs hits : matchingDocs) {
      if (ReaderUtil.getTopLevelContext(hits.context()).reader() != reader) {
        throw new IllegalStateException(
            "the SortedSetDocValuesReaderState provided to this class "
                + "does not match the reader being searched;"
                + " you must create a new SortedSetDocValuesReaderState "
                + "every time you open a new IndexReader");
      }

      countOneSegment(ordinalMap, hits.context().reader(), hits.context().ord, hits, null);
    }
  }

  private void increment(int ordinal) {
    increment(ordinal, 1);
  }

  private void increment(int ordinal, int amount) {
    if (this.sparseCounts != null) {
      this.sparseCounts.addTo(ordinal, amount);
    } else {
      Check.isNotNull(this.denseCounts, "counts")[ordinal] += amount;
    }
  }
}
