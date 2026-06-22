package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.cursor.CursorConfig;
import java.time.Duration;
import java.util.Optional;

/** Maps the {@code cursor} config block onto the runtime {@link CursorConfig}. */
public final class CursorConfigMapper {

  private CursorConfigMapper() {}

  /** Builds a {@link CursorConfig} from the parsed {@code cursor} block. */
  public static CursorConfig toCursorConfig(Optional<CommunityCursorConfig> cursorConfig) {
    return CursorConfig.create(
        cursorConfig
            .flatMap(CommunityCursorConfig::idleCursorHandlingRateMs)
            .map(Duration::ofMillis),
        cursorConfig.flatMap(CommunityCursorConfig::cursorIdleTimeMs).map(Duration::ofMillis),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }
}
