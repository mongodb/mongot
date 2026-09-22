package com.xgen.mongot.replication.mongodb.initialsync;

import static com.google.common.truth.Truth.assertThat;

import com.ibm.icu.text.RuleBasedCollator;
import java.util.Optional;
import org.bson.BsonDocument;
import org.junit.Test;

public class CollationUtilsTest {

  @Test
  public void toIcuCollator_null_returnsEmpty() {
    assertThat(CollationUtils.toIcuCollator(null)).isEmpty();
  }

  @Test
  public void toIcuCollator_simpleLocale_returnsEmpty() {
    // The simple (byte-ordered) collation is already produced by the UTF-8 base comparator.
    assertThat(CollationUtils.toIcuCollator(BsonDocument.parse("{locale:\"simple\"}"))).isEmpty();
  }

  @Test
  public void toIcuCollator_strengthSecond_ordersCaseInsensitively() {
    Optional<RuleBasedCollator> collator =
        CollationUtils.toIcuCollator(BsonDocument.parse("{locale:\"en\",strength:2}"));

    assertThat(collator).isPresent();
    // Case-insensitive (secondary) order: 'b' before 'C', matching the collation order the resync
    // sort-merge relies on. Binary (String.compareTo) would order 'C' before 'b'.
    assertThat(collator.get().compare("b", "C")).isLessThan(0);
    assertThat(collator.get().compare("C", "b")).isGreaterThan(0);
  }

  @Test
  public void toIcuCollator_defaultStrength_ordersByBaseLetter() {
    // TERTIARY (mongod default) compares base letters before case: 'C' (base 'c') sorts after 'b'.
    // Binary (String.compareTo) would order 'C' before 'b' because 0x43 < 0x62.
    RuleBasedCollator collator =
        CollationUtils.toIcuCollator(BsonDocument.parse("{locale:\"en\"}")).get();
    assertThat(collator.compare("C", "b")).isGreaterThan(0);
  }

  @Test
  public void toIcuCollator_numericOrdering_sortsNumbersNumerically() {
    Optional<RuleBasedCollator> collator =
        CollationUtils.toIcuCollator(BsonDocument.parse("{locale:\"en\",numericOrdering:true}"));

    assertThat(collator).isPresent();
    assertThat(collator.get().compare("a2", "a10")).isLessThan(0);
  }

  @Test
  public void toIcuCollator_strengthThreeCaseFirstUpper_ordersUppercaseFirst() {
    // MongoDB compares case at strength 3 (tertiary) regardless of caseLevel, so caseFirst applies:
    // uppercase 'A' sorts before lowercase 'a'.
    RuleBasedCollator collator =
        CollationUtils.toIcuCollator(
                BsonDocument.parse("{locale:\"en\",strength:3,caseFirst:\"upper\"}"))
            .get();
    assertThat(collator.compare("A", "a")).isLessThan(0);
    assertThat(collator.compare("a", "A")).isGreaterThan(0);
  }

  @Test
  public void toIcuCollator_strengthTwoNoCaseLevel_caseFirstIgnored() {
    // At strength 2 with caseLevel:false there is no case to order, so caseFirst is ignored: 'A'
    // and 'a' compare equal (case-insensitive), matching the server.
    RuleBasedCollator collator =
        CollationUtils.toIcuCollator(
                BsonDocument.parse("{locale:\"en\",strength:2,caseFirst:\"upper\"}"))
            .get();
    assertThat(collator.compare("A", "a")).isEqualTo(0);
  }
}
