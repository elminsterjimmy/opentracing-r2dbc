package opentracing.r2dbc.common;

import java.util.Set;

public class TracingConfiguration {

  private boolean traceEnabled;

  private Set<String> ignoreStatements;

  private long slowQueryThresholdMs;

  public long getSlowQueryThresholdMs() {
    return slowQueryThresholdMs;
  }

  public void setSlowQueryThresholdMs(long slowQueryThresholdMs) {
    this.slowQueryThresholdMs = slowQueryThresholdMs;
  }

  public boolean isTraceEnabled() {
    return traceEnabled;
  }

  public void setTraceEnabled(boolean traceEnabled) {
    this.traceEnabled = traceEnabled;
  }

  public Set<String> getIgnoreStatements() {
    return ignoreStatements;
  }

  public void setIgnoreStatements(Set<String> ignoreStatements) {
    this.ignoreStatements = ignoreStatements;
  }

  public static final class TracingConfigurationBuilder {
    private boolean traceEnabled;
    private Set<String> ignoreStatements;
    private long slowQueryThresholdMs;

    private TracingConfigurationBuilder() {
    }

    public static TracingConfigurationBuilder aTracingConfiguration() {
      return new TracingConfigurationBuilder();
    }

    public TracingConfigurationBuilder withTraceEnabled(boolean traceEnabled) {
      this.traceEnabled = traceEnabled;
      return this;
    }

    public TracingConfigurationBuilder withIgnoreStatements(Set<String> ignoreStatements) {
      this.ignoreStatements = ignoreStatements;
      return this;
    }

    public TracingConfigurationBuilder withSlowQueryThresholdMs(long slowQueryThresholdMs) {
      this.slowQueryThresholdMs = slowQueryThresholdMs;
      return this;
    }

    public TracingConfiguration build() {
      TracingConfiguration tracingConfiguration = new TracingConfiguration();
      tracingConfiguration.setTraceEnabled(traceEnabled);
      tracingConfiguration.setIgnoreStatements(ignoreStatements);
      tracingConfiguration.setSlowQueryThresholdMs(slowQueryThresholdMs);
      return tracingConfiguration;
    }
  }
}
