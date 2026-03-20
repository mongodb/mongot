package com.xgen.mongot.cursor;

import com.xgen.mongot.util.Bytes;
import com.xgen.testing.BsonSerializationTestSuite;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {CursorConfigTest.SerializationTest.class})
public class CursorConfigTest {

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "cursor-config-serialization";
    private static final BsonSerializationTestSuite<CursorConfig> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable("src/test/unit/resources/cursor", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<CursorConfig> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<CursorConfig> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<CursorConfig>> data() {
      return Collections.singletonList(fullConfig());
    }

    private static BsonSerializationTestSuite.TestSpec<CursorConfig> fullConfig() {
      return BsonSerializationTestSuite.TestSpec.create(
          "full config",
          CursorConfig.create(
              Optional.of(Duration.ofMillis(100)),
              Optional.of(Duration.ofMinutes(1)),
              Optional.of(Bytes.ofBytes(256)),
              Optional.of(Bytes.ofBytes(512)),
              Optional.of(Bytes.ofBytes(1024)),
              Optional.of(Range.of(13L, 30L))));
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }
}
