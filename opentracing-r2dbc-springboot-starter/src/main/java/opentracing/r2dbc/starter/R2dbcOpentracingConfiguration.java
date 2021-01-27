package opentracing.r2dbc.starter;


import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Set;

@ConfigurationProperties(prefix = "opentracing.r2dbc")
public class R2dbcOpentracingConfiguration {

  private boolean enabled;
  private boolean showSlowSql;
  private long slowQueryThresholdMs;
  private Set<String> ignoreStatements;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isShowSlowSql() {
    return showSlowSql;
  }

  public void setShowSlowSql(boolean showSlowSql) {
    this.showSlowSql = showSlowSql;
  }

  public long getSlowQueryThresholdMs() {
    return slowQueryThresholdMs;
  }

  public void setSlowQueryThresholdMs(long slowQueryThresholdMs) {
    this.slowQueryThresholdMs = slowQueryThresholdMs;
  }

  public Set<String> getIgnoreStatements() {
    return ignoreStatements;
  }

  public void setIgnoreStatements(Set<String> ignoreStatements) {
    this.ignoreStatements = ignoreStatements;
  }
}
