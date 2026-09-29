package com.xgen.mongot.util;

/** Identifies how mongot was bootstrapped, used to gate deployment-specific features. */
public enum DeploymentEnvironment {
  ATLAS("mongot-atlas"),
  COMMUNITY("mongot-community"),
  ENTERPRISE("mongot-enterprise"),
  LOCAL_DEV("mongot-localdev"),
  TEST("mongot-test");

  private final String deploymentEnvironmentName;

  DeploymentEnvironment(String deploymentEnvironmentName) {
    this.deploymentEnvironmentName = deploymentEnvironmentName;
  }

  /**
   * The name mongot reports about itself in explain metadata and outbound HTTP user agents, e.g.
   * {@code "mongot-community"}.
   */
  public String deploymentEnvironmentName() {
    return this.deploymentEnvironmentName;
  }
}
