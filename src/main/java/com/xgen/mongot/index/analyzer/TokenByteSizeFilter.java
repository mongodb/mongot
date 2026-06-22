package com.xgen.mongot.index.analyzer;

import com.google.common.flogger.FluentLogger;
import com.google.common.flogger.MetadataKey;
import com.xgen.mongot.util.Check;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.IndexWriter;

/**
 * TokenByteSizeFilter removes terms whose UTF-8 encoding's bytes are longer than maxByteSize, which
 * defaults to IndexWriter.MAX_TERM_LENGTH.
 */
public final class TokenByteSizeFilter extends FilteringTokenFilter {

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private static final MetadataKey<String> FIELD_NAME =
      MetadataKey.single("fieldName", String.class);
  private static final MetadataKey<Integer> MAX_BYTE_SIZE =
      MetadataKey.single("maxByteSize", Integer.class);
  private static final MetadataKey<Integer> TOKEN_SIZE =
      MetadataKey.single("tokenSize", Integer.class);

  private final int maxByteSize;
  private final String fieldName;

  private final TermToBytesRefAttribute termAtt = addAttribute(TermToBytesRefAttribute.class);

  /**
   * Constructs a new TokenByteSizeFilter only accepting terms smaller than the provided
   * maxByteSize.
   */
  public TokenByteSizeFilter(TokenStream in, String fieldName, int maxByteSize) {
    super(in);

    Check.argIsPositive(maxByteSize, "maxByteSize");
    Check.argNotNull(fieldName, "fieldName");
    this.maxByteSize = maxByteSize;
    this.fieldName = fieldName;
  }

  public TokenByteSizeFilter(TokenStream in, String fieldName) {
    this(in, fieldName, IndexWriter.MAX_TERM_LENGTH);
  }

  @Override
  protected boolean accept() {
    int len = this.termAtt.getBytesRef().length;
    boolean tooLong = len > this.maxByteSize;
    if (tooLong) {
      FLOGGER
          .atWarning()
          .atMostEvery(1, TimeUnit.MINUTES)
          .with(FIELD_NAME, this.fieldName)
          .with(MAX_BYTE_SIZE, this.maxByteSize)
          .with(TOKEN_SIZE, len)
          .log(
              "Unable to index token in field due to it being larger than the maximum byte length");
    }

    return !tooLong;
  }
}
