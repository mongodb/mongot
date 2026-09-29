package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.util.DeploymentEnvironment;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.Test;

public class CursorConfigMapperTest {

  private static final Path TUNING_CONFIG =
      Path.of("src/test/unit/resources/config/provider/community/communityConfigTuning.yaml");

  @Test
  public void toCursorConfig_honorsConfiguredValues() throws Exception {
    CommunityConfig config =
        CommunityConfig.readFromFile(TUNING_CONFIG, Optional.of(DeploymentEnvironment.COMMUNITY))
            .config();

    CursorConfig cursorConfig =
        CursorConfigMapper.toCursorConfig(
            config.advancedConfigs().flatMap(AdvancedConfigs::cursorConfig));

    var bson = cursorConfig.toBson();
    assertEquals(1800000, bson.getInt32("idleCursorHandlingRateMs").getValue());
    assertEquals(3600000, bson.getInt32("cursorIdleTimeMs").getValue());
  }

  @Test
  public void toCursorConfig_appliesDefaultsWhenUnset() {
    CursorConfig cursorConfig = CursorConfigMapper.toCursorConfig(Optional.empty());

    var defaults = CursorConfig.getDefault().toBson();
    assertEquals(defaults, cursorConfig.toBson());
  }
}
