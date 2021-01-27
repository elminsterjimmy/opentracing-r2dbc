package opentracing.r2dbc.starter;

import io.opentracing.Tracer;
import io.r2dbc.proxy.listener.ProxyMethodExecutionListener;
import opentracing.r2dbc.common.OpenTracingExecutorListener;
import opentracing.r2dbc.common.TracingConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnBean(value = Tracer.class)
@EnableConfigurationProperties(R2dbcOpentracingConfiguration.class)
@ConditionalOnProperty(prefix = "opentracing.r2dbc", name = "enabled", havingValue = "true")
public class R2dbcOpentracingAutoConfig {

  private final R2dbcOpentracingConfiguration r2dbcOpentracingConfiguration;
  private final Tracer tracer;

  @Autowired
  public R2dbcOpentracingAutoConfig(R2dbcOpentracingConfiguration r2dbcOpentracingConfiguration, Tracer tracer) {
    this.r2dbcOpentracingConfiguration = r2dbcOpentracingConfiguration;
    this.tracer = tracer;
  }

  @Bean
  public ProxyMethodExecutionListener getR2dbcOpenTracer() {
    return new OpenTracingExecutorListener(tracer, TracingConfiguration.TracingConfigurationBuilder
        .aTracingConfiguration()
        .withIgnoreStatements(r2dbcOpentracingConfiguration.getIgnoreStatements())
        .withTraceEnabled(r2dbcOpentracingConfiguration.isEnabled())
        .withSlowQueryThresholdMs(r2dbcOpentracingConfiguration.isShowSlowSql() ?
            r2dbcOpentracingConfiguration.getSlowQueryThresholdMs() : 0)
        .build());
  }
}
