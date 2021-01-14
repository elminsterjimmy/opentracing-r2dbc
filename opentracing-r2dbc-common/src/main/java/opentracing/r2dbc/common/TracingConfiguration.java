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
}
