package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.testing.Helpers;
import java.util.List;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.Test;

public class ResumeTokenUtilsTest {

  // _data is a hex-encoded KeyString, most significant field first:
  //   82 | clusterTime (8 bytes) | version | tokenType | txnOpIndex | fromInvalidate | documentKey
  // Small ints encode as 29 (zero) or 2B02 (one).
  private static final String CLUSTER_TIME = "68D0A4B400000001";
  private static final String LATER_CLUSTER_TIME = "68D0A4B500000001";
  private static final String VERSION = "2B02";
  private static final String DOCUMENT_KEY = "2C0455";

  private static final String EVENT = event(CLUSTER_TIME, "29");
  private static final String EVENT_NEXT_IN_TXN = event(CLUSTER_TIME, "2B02");
  private static final String EVENT_LATER_OPTIME = event(LATER_CLUSTER_TIME, "29");

  /** A high water mark token: tokenType zero, truncated after it. */
  private static String highWaterMark(String clusterTime) {
    return "82" + clusterTime + VERSION + "29";
  }

  /** An event token at the given encoded txnOpIndex. */
  private static String event(String clusterTime, String txnOpIndex) {
    return "82" + clusterTime + VERSION + "2B02" + txnOpIndex + "6E" + DOCUMENT_KEY;
  }

  @Test
  public void byStreamPosition_differentClusterTimes_ordersByClusterTime() {
    assertThat(compare(EVENT, EVENT_LATER_OPTIME)).isLessThan(0);
    assertThat(compare(EVENT_LATER_OPTIME, EVENT)).isGreaterThan(0);
  }

  @Test
  public void byStreamPosition_sameClusterTime_ordersByTxnOpIndex() throws DecoderException {
    // The distinguishing case: different positions in one transaction, equal optimes.
    assertThat(opTime(EVENT)).isEqualTo(opTime(EVENT_NEXT_IN_TXN));

    assertThat(compare(EVENT, EVENT_NEXT_IN_TXN)).isLessThan(0);
    assertThat(compare(EVENT_NEXT_IN_TXN, EVENT)).isGreaterThan(0);
  }

  @Test
  public void byStreamPosition_highWaterMarkAgainstEventAtSameClusterTime_ordersHighWaterMarkFirst()
      throws DecoderException {
    // Idle partitions report high water marks while busy ones report events, so the low water
    // mark computation mixes both shapes.
    String idle = highWaterMark(CLUSTER_TIME);
    assertThat(opTime(idle)).isEqualTo(opTime(EVENT));

    assertThat(compare(idle, EVENT)).isLessThan(0);
    assertThat(compare(EVENT, idle)).isGreaterThan(0);
  }

  @Test
  public void byStreamPosition_mixedCaseWithinOneToken_ordersByPositionNotByCase() {
    // Mixes upper- and lowercase hex digits within a single token, rather than one case per token.
    String mixedCaseEvent = mixCase(EVENT);
    String mixedCaseEventNextInTxn = mixCase(EVENT_NEXT_IN_TXN);

    assertThat(compare(mixedCaseEvent, mixedCaseEventNextInTxn)).isLessThan(0);
    assertThat(compare(mixedCaseEventNextInTxn, mixedCaseEvent)).isGreaterThan(0);
  }

  @Test
  public void byStreamPosition_highWaterMarkAheadOfEvent_ordersByClusterTime() {
    assertThat(compare(EVENT, highWaterMark(LATER_CLUSTER_TIME))).isLessThan(0);
    assertThat(compare(highWaterMark(LATER_CLUSTER_TIME), EVENT)).isGreaterThan(0);
  }

  @Test
  public void byStreamPosition_lowercaseData_ordersByPositionNotByCase() {
    // Under String#compareTo a lowercase digit outranks every uppercase one, inverting the order.
    assertThat(compare(EVENT.toLowerCase(), EVENT_LATER_OPTIME)).isLessThan(0);
    assertThat(compare(EVENT_LATER_OPTIME.toLowerCase(), EVENT)).isGreaterThan(0);
  }

  @Test
  public void byStreamPosition_sameDataDifferingInCase_comparesEqual() {
    assertThat(compare(EVENT, EVENT.toLowerCase())).isEqualTo(0);
    assertThat(compare(EVENT.toLowerCase(), EVENT)).isEqualTo(0);
  }

  @Test
  public void byStreamPosition_identicalTokens_comparesEqual() {
    assertThat(compare(EVENT, EVENT)).isEqualTo(0);
  }

  @Test
  public void byStreamPosition_ignoresFieldsOtherThanData() {
    BsonDocument withExtraField = resumeToken(EVENT).append("unrelated", new BsonInt32(1));

    assertThat(ResumeTokenUtils.BY_STREAM_POSITION.compare(resumeToken(EVENT), withExtraField))
        .isEqualTo(0);
  }

  @Test
  public void byStreamPosition_acrossTokenShapes_isATotalOrder() {
    Helpers.testComparator(
        ResumeTokenUtils.BY_STREAM_POSITION,
        List.of(
            resumeToken(highWaterMark(CLUSTER_TIME)),
            resumeToken(EVENT),
            resumeToken(EVENT_NEXT_IN_TXN),
            resumeToken(highWaterMark(LATER_CLUSTER_TIME)),
            resumeToken(EVENT_LATER_OPTIME)));
  }

  @Test
  public void byStreamPosition_usedAsMinimum_selectsFurthestBehindToken() {
    List<BsonDocument> tokens =
        List.of(
            resumeToken(EVENT_LATER_OPTIME), resumeToken(EVENT), resumeToken(EVENT_NEXT_IN_TXN));

    BsonDocument lowWaterMark =
        tokens.stream().min(ResumeTokenUtils.BY_STREAM_POSITION).orElseThrow();

    assertThat(lowWaterMark).isEqualTo(resumeToken(EVENT));
  }

  @Test
  public void
      byStreamPosition_dataFieldTooShortForAStreamPosition_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ResumeTokenUtils.BY_STREAM_POSITION.compare(resumeToken(""), resumeToken(EVENT)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ResumeTokenUtils.BY_STREAM_POSITION.compare(
                resumeToken(CLUSTER_TIME), resumeToken(EVENT)));
  }

  @Test
  public void byStreamPosition_dataFieldOddLength_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ResumeTokenUtils.BY_STREAM_POSITION.compare(
                resumeToken(EVENT + "0"), resumeToken(EVENT)));
  }

  @Test
  public void byStreamPosition_dataFieldNotHex_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ResumeTokenUtils.BY_STREAM_POSITION.compare(
                resumeToken(EVENT.substring(0, EVENT.length() - 2) + "zz"), resumeToken(EVENT)));
  }

  @Test
  public void byStreamPosition_nullToken_throwsIllegalArgumentException() {
    // Diverges from the Comparator convention of NullPointerException; asserted so it stays
    // deliberate.
    assertThrows(
        IllegalArgumentException.class,
        () -> ResumeTokenUtils.BY_STREAM_POSITION.compare(null, resumeToken(EVENT)));
  }

  @Test
  public void dataFromResumeToken_returnsEncodedData() {
    assertThat(ResumeTokenUtils.dataFromResumeToken(resumeToken(EVENT))).isEqualTo(EVENT);
  }

  @Test
  public void dataFromResumeToken_shortDataField_returnsItUnvalidated() {
    // Length is unchecked here so malformed tokens still fail at the decode step.
    assertThat(ResumeTokenUtils.dataFromResumeToken(resumeToken("8268D0A4"))).isEqualTo("8268D0A4");
  }

  @Test
  public void dataFromResumeToken_nullToken_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> ResumeTokenUtils.dataFromResumeToken(null));
  }

  @Test
  public void dataFromResumeToken_missingDataField_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ResumeTokenUtils.dataFromResumeToken(new BsonDocument()));
  }

  @Test
  public void dataFromResumeToken_dataFieldNotAString_throwsIllegalArgumentException() {
    BsonDocument notAString = new BsonDocument("_data", new BsonInt32(1));

    assertThrows(
        IllegalArgumentException.class, () -> ResumeTokenUtils.dataFromResumeToken(notAString));
  }

  @Test
  public void lowWaterMark_singleToken_returnsIt() {
    assertThat(ResumeTokenUtils.lowWaterMark(List.of(resumeToken(EVENT))))
        .isEqualTo(resumeToken(EVENT));
  }

  @Test
  public void lowWaterMark_minimumInMiddle_returnsEarliest() {
    assertThat(
            ResumeTokenUtils.lowWaterMark(
                List.of(
                    resumeToken(EVENT_NEXT_IN_TXN),
                    resumeToken(EVENT),
                    resumeToken(EVENT_LATER_OPTIME))))
        .isEqualTo(resumeToken(EVENT));
  }

  @Test
  public void lowWaterMark_sameTransaction_ordersByFullStreamPosition() {
    // Same clusterTime, so only the full _data tells them apart.
    assertThat(
            ResumeTokenUtils.lowWaterMark(
                List.of(resumeToken(EVENT_NEXT_IN_TXN), resumeToken(EVENT))))
        .isEqualTo(resumeToken(EVENT));
  }

  @Test
  public void lowWaterMark_empty_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> ResumeTokenUtils.lowWaterMark(List.of()));
  }

  @Test
  public void opTimeFromResumeToken_returnsClusterTime() throws DecoderException {
    assertThat(opTime(EVENT)).isEqualTo(new BsonTimestamp(0x68D0A4B400000001L));
  }

  @Test
  public void opTimeFromResumeToken_emptyDataField_throwsIllegalArgumentException() {
    // Empty _data decodes to a zero-length array, so the length check must precede bytes[0].
    assertThrows(IllegalArgumentException.class, () -> opTime(""));
  }

  @Test
  public void opTimeFromResumeToken_notATimestamp_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> opTime("00" + CLUSTER_TIME));
  }

  @Test
  public void opTimeFromResumeToken_notValidHex_throwsDecoderException() {
    assertThrows(DecoderException.class, () -> opTime("not-valid-hex"));
  }

  private static int compare(String leftData, String rightData) {
    return ResumeTokenUtils.BY_STREAM_POSITION.compare(
        resumeToken(leftData), resumeToken(rightData));
  }

  private static BsonDocument resumeToken(String data) {
    return new BsonDocument("_data", new BsonString(data));
  }

  private static BsonTimestamp opTime(String data) throws DecoderException {
    return ResumeTokenUtils.opTimeFromResumeToken(resumeToken(data));
  }

  /** Alternates the case of each hex digit, so a single token mixes upper- and lowercase. */
  private static String mixCase(String data) {
    StringBuilder mixed = new StringBuilder(data.length());
    for (int i = 0; i < data.length(); i++) {
      char c = data.charAt(i);
      mixed.append(i % 2 == 0 ? Character.toUpperCase(c) : Character.toLowerCase(c));
    }
    return mixed.toString();
  }
}
