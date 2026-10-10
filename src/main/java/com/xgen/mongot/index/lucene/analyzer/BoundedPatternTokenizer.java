/*
 * Derived from Apache Lucene 10.4.0 PatternTokenizer.
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

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.regex.RegexBudget;
import com.xgen.mongot.util.regex.RegexLimitExceededException;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

/** Lucene's split/capture tokenizer with bounded input reads and matcher-local stack protection. */
final class BoundedPatternTokenizer extends Tokenizer {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final StringBuilder text = new StringBuilder();
  private final char[] buffer = new char[8192];
  private final Matcher matcher;
  private final int group;
  private int index;

  BoundedPatternTokenizer(Pattern pattern, int group) {
    this.matcher = pattern.matcher("");
    this.group = group;
    if (group >= 0 && group > this.matcher.groupCount()) {
      throw new IllegalArgumentException(
          "invalid group specified: pattern only has: "
              + this.matcher.groupCount()
              + " capturing groups");
    }
  }

  private boolean find() {
    RegexBudget.checkInterrupted();
    try {
      return this.matcher.find();
    } catch (StackOverflowError e) {
      // Convert here, before a VM Error can escape into Lucene's write/cleanup machinery.
      throw new RegexLimitExceededException();
    }
  }

  @Override
  public boolean incrementToken() {
    if (this.index >= this.text.length()) {
      return false;
    }
    clearAttributes();
    if (this.group >= 0) {
      while (find()) {
        this.index = this.matcher.start(this.group);
        int end = this.matcher.end(this.group);
        if (this.index == end) {
          continue;
        }
        this.termAtt.setEmpty().append(this.text, this.index, end);
        this.offsetAtt.setOffset(correctOffset(this.index), correctOffset(end));
        return true;
      }
      this.index = Integer.MAX_VALUE;
      return false;
    }

    while (find()) {
      if (this.matcher.start() - this.index > 0) {
        this.termAtt.setEmpty().append(this.text, this.index, this.matcher.start());
        this.offsetAtt.setOffset(correctOffset(this.index), correctOffset(this.matcher.start()));
        this.index = this.matcher.end();
        return true;
      }
      this.index = this.matcher.end();
    }
    if (this.text.length() - this.index == 0) {
      this.index = Integer.MAX_VALUE;
      return false;
    }
    this.termAtt.setEmpty().append(this.text, this.index, this.text.length());
    this.offsetAtt.setOffset(correctOffset(this.index), correctOffset(this.text.length()));
    this.index = Integer.MAX_VALUE;
    return true;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    this.text.setLength(0);
    @Var int length;
    while ((length = this.input.read(this.buffer)) > 0) {
      this.text.append(this.buffer, 0, length);
    }
    this.matcher.reset(RegexBudget.forStage().wrap(this.text));
    this.index = 0;
  }

  @Override
  public void end() throws IOException {
    super.end();
    int offset = correctOffset(this.text.length());
    this.offsetAtt.setOffset(offset, offset);
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      this.matcher.reset("");
      this.text.setLength(0);
      this.text.trimToSize();
    }
  }
}
