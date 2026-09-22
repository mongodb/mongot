package com.xgen.mongot.replication.mongodb.initialsync;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Maps a MongoDB collation spec, as reported by {@code listCollections}, to an ICU collator that
 * reproduces the same string ordering.
 *
 * <p>The resync sort-merge compares string {@code _id}s to interleave the source-cursor order and
 * the mat-view order. Both are ordered by the server in the collection's collation, so mongot's
 * merge has to agree. The simple collation (or an absent spec) is byte-ordered (UTF-8), which
 * {@link MqlComparator} already produces; only a non-simple collation needs ICU.
 *
 * <p>This mirrors the mapping validated against mongod in mongot's collation-parity harness. It is
 * not universally bit-for-bit with mongod (e.g. {@code th} diverges from the bundled ICU version);
 * the collation-order tripwire (CLOUDP-447390) covers locales with no parity.
 */
final class CollationUtils {

  private CollationUtils() {}

  /**
   * Returns an ICU collator for the given collation spec, or empty for the simple collation (or a
   * null/absent spec).
   */
  static Optional<RuleBasedCollator> toIcuCollator(BsonDocument collation) {
    if (collation == null) {
      return Optional.empty();
    }
    String locale = strOr(collation, "locale", "simple");
    if ("simple".equals(locale)) {
      return Optional.empty();
    }
    int strength = intOr(collation, "strength", 3); // mongod default is strength 3
    RuleBasedCollator collator;
    try {
      collator = (RuleBasedCollator) Collator.getInstance(new ULocale(locale));
      // A locale ICU can't map to a RuleBasedCollator (e.g. a lenient ULocale that resolves to a
      // non-standard collator subclass) would throw ClassCastException on the cast above; treat it
      // as unsupported rather than propagating.
    } catch (IllegalArgumentException | ClassCastException e) {
      return Optional.empty();
    }
    int strengthIndex = Math.max(1, Math.min(MONGO_TO_ICU_STRENGTH.length, strength)) - 1;
    collator.setStrength(MONGO_TO_ICU_STRENGTH[strengthIndex]);
    // MongoDB compares case at strength 3+ (tertiary+) regardless of caseLevel, and at strength 1-2
    // only when caseLevel is true. So caseFirst applies when caseLevel is on OR strength >= 3; a
    // strength-1/2 + caseLevel:false collation has no case to order (caseFirst is ignored).
    boolean caseLevel = boolOr(collation, "caseLevel", false);
    if (caseLevel) {
      collator.setCaseLevel(true);
    }
    String caseFirst = strOr(collation, "caseFirst", "off");
    if (!"off".equals(caseFirst) && (caseLevel || strength >= 3)) {
      if ("upper".equals(caseFirst)) {
        collator.setUpperCaseFirst(true);
      } else if ("lower".equals(caseFirst)) {
        collator.setLowerCaseFirst(true);
      }
    }
    collator.setNumericCollation(boolOr(collation, "numericOrdering", false));
    collator.setFrenchCollation(boolOr(collation, "backwards", false));
    collator.setDecomposition(
        boolOr(collation, "normalization", false)
            ? Collator.CANONICAL_DECOMPOSITION
            : Collator.NO_DECOMPOSITION);
    // TODO(CLOUDP-448794): maxVariable is intentionally not mapped. The driver half is easy
    // (CollationMaxVariable), but ICU's setMaxVariable takes undocumented, version-internal
    // "variable group" values (no named SPACE/PUNCT constant in ICU4J 77.1; 0x1001 ~= punct,
    // 0x1000 ~= space), so the ICU side would need hardcoded magic integers that shift on an ICU
    // bump. It must be mapped identically in BOTH the mat-view collation and this comparator, or
    // they disagree (worse than not mapping). A source with alternate:"shifted" +
    // maxVariable:"space" is therefore a non-parity config; observe the affected set
    // (CLOUDP-444645,
    // gauge + full collation in logs) and the resync collation-order tripwire (CLOUDP-447390)
    // detect the divergence, so prioritize this ticket by actual usage.
    if ("shifted".equals(strOr(collation, "alternate", "non-ignorable"))) {
      collator.setAlternateHandlingShifted(true);
    }
    return Optional.of(collator);
  }

  // MongoDB strength 1..5 -> ICU PRIMARY(0)/SECONDARY(1)/TERTIARY(2)/QUATERNARY(3)/IDENTICAL(15).
  private static final int[] MONGO_TO_ICU_STRENGTH = {
    Collator.PRIMARY, Collator.SECONDARY, Collator.TERTIARY, Collator.QUATERNARY, Collator.IDENTICAL
  };

  private static String strOr(BsonDocument doc, String key, String def) {
    BsonValue v = doc.get(key);
    return v instanceof BsonString ? v.asString().getValue() : def;
  }

  private static int intOr(BsonDocument doc, String key, int def) {
    // Accept any BSON numeric type for a numeric option (e.g. strength): a listCollections /
    // conf-call deserialization can surface it as int32, int64 or double, and a BsonInt32-only
    // check
    // would silently fall back to the default.
    BsonValue v = doc.get(key);
    return v instanceof BsonNumber ? ((BsonNumber) v).intValue() : def;
  }

  private static boolean boolOr(BsonDocument doc, String key, boolean def) {
    BsonValue v = doc.get(key);
    return v instanceof BsonBoolean ? v.asBoolean().getValue() : def;
  }
}
