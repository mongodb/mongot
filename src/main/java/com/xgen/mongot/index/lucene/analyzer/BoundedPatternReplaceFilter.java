/*
 * Derived from Apache Lucene 10.4.0 PatternReplaceFilter.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xgen.mongot.index.lucene.analyzer;

import com.xgen.mongot.util.regex.RegexBudget;
import com.xgen.mongot.util.regex.RegexLimitExceededException;
import java.io.IOException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.jetbrains.annotations.Nullable;

/** Preserves Java replacement semantics while charging all tokens to one stage budget. */
final class BoundedPatternReplaceFilter extends TokenFilter {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final Matcher matcher;
  private final String replacement;
  private final boolean all;
  private @Nullable RegexBudget budget;

  BoundedPatternReplaceFilter(TokenStream input, Pattern pattern, String replacement, boolean all) {
    super(input);
    this.matcher = pattern.matcher("");
    this.replacement = replacement == null ? "" : replacement;
    this.all = all;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    this.budget = RegexBudget.forStage();
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (!this.input.incrementToken()) {
      return false;
    }
    this.matcher.reset(
        Objects.requireNonNull(this.budget, "reset must precede incrementToken")
            .wrap(this.termAtt));
    try {
      if (this.matcher.find()) {
        // replaceAll/replaceFirst reset the Matcher, but must not replenish its read budget.
        String transformed =
            this.all
                ? this.matcher.replaceAll(this.replacement)
                : this.matcher.replaceFirst(this.replacement);
        this.termAtt.setEmpty().append(transformed);
      }
    } catch (StackOverflowError e) {
      throw new RegexLimitExceededException();
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      this.matcher.reset("");
      this.budget = null;
    }
  }
}
