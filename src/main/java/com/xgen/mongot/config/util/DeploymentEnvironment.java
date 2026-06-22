package com.xgen.mongot.config.util;

/** Identifies how mongot was bootstrapped, used to gate deployment-specific features. */
public enum DeploymentEnvironment {
  ATLAS,
  COMMUNITY,
  LOCAL_DEV,
  TEST
}
