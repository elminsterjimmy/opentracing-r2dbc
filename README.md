# OpenTracing for R2DBC

An OpenTracing instrumentation for R2DBC

## Usage

```java
ConnectionFactory original = ...

ConnectionFactory connectionFactory = ProxyConnectionFactory.builder(original)
    .listener(new OpenTracingExecutorListener())  // add listener
    .build();

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();

// Alternative: Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

## Spring integration

* ##### Maven dependency:
```maven
<dependency>
  <groupId>io.opentracing.contrib</groupId>
  <artifactId>opentracing-r2dbc-springboot-starter</artifactId>
  <version>${opentracing-r2dbc-version}</version>
</dependency>
```

* ##### ConnectionFactory initialization:
```java
@Bean
public ConnectionFactory connectionFactory() {
  ConnectionFactory original = ...
  ProxyConnectionFactory.Builder builder = ProxyConnectionFactory.builder(connectionPool);
  if (null != proxyExecutionListener) { // in case of r2dbc opentracing is not enabled
    builder.listener(proxyExecutionListener);
  }
  ConnectionFactory proxyConnectionFactory = builder.build();
  return proxyConnectionFactory;
}
```
* ##### spring configuration:
```yaml
opentracing:
  r2dbc:
    enabled: ${R2DBC_OPENTRACING_ENABLED:false}
    showSlowSql: ${R2DBC_OPENTRACING_SHOW_SLOW_SQL:false}
    slowQueryThresholdMs: ${R2DBC_OPENTRACING_SHOW_QUERY_THRESHOLD:0}
    ignoreStatements: ${R2DBC_OPENTRACING_IGNORE_STATEMENTS:}
```
---
Based on [r2dbc-proxy](https://github.com/r2dbc/r2dbc-proxy)
