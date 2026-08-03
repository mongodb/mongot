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
package com.xgen.mongot.index.lucene.query.sort.comparator;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.search.AbstractDocIdSetIterator;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;

/**
 * Copy of Lucene 10.4's {@code org.apache.lucene.search.comparators.UpdateableDocIdSetIterator},
 * which is package-private and not accessible from mongot. {@link MongotNumericComparator} needs an
 * updateable wrapper to swap competitive iterators in place; this class fulfils the same contract.
 *
 * <p>Lint/style checks are suppressed for this file (like MongotNumericComparator) so the body
 * stays byte-identical to upstream; on Lucene upgrade, re-dump the upstream contents here.
 */
final class MongotUpdateableDocIdSetIterator extends AbstractDocIdSetIterator {

  private DocIdSetIterator in = DocIdSetIterator.empty();

  /**
   * Update the wrapped {@link DocIdSetIterator}. It doesn't need to be positioned on the same doc
   * ID as this iterator.
   */
  void update(DocIdSetIterator iterator) {
    this.in = Objects.requireNonNull(iterator);
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(doc + 1);
  }

  @Override
  public int advance(int target) throws IOException {
    int curDoc = in.docID();
    if (curDoc < target) {
      curDoc = in.advance(target);
    }
    return this.doc = curDoc;
  }

  @Override
  public long cost() {
    return in.cost();
  }

  @Override
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    // #update may have been just called
    if (in.docID() < doc) {
      in.advance(doc);
    }
    in.intoBitSet(upTo, bitSet, offset);
    doc = in.docID();
  }

  @Override
  public int docIDRunEnd() throws IOException {
    // #update may have been just called
    if (in.docID() < doc) {
      in.advance(doc);
    }
    if (in.docID() == doc) {
      return in.docIDRunEnd();
    } else {
      return super.docIDRunEnd();
    }
  }
}
