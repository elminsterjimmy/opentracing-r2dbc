package opentracing.r2dbc.common;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;
import io.r2dbc.proxy.core.ConnectionInfo;
import io.r2dbc.proxy.core.ExecutionType;
import io.r2dbc.proxy.core.MethodExecutionInfo;
import io.r2dbc.proxy.core.QueryExecutionInfo;
import io.r2dbc.spi.ConnectionMetadata;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static opentracing.r2dbc.common.TagConstants.*;

public class SpanUtils {

  private static final String COMPONENT_NAME = "opentracing-r2dbc";

  public static void finishSpan(Span span,
                                QueryExecutionInfo queryExecutionInfo,
                                TracingConfiguration tracingConfiguration) {
    _finishSpan(span,
        tracingConfiguration,
        queryExecutionInfo.getExecuteDuration(),
        queryExecutionInfo.getConnectionInfo(),
        queryExecutionInfo.getThreadId(),
        queryExecutionInfo.getThreadName());
  }

  private static void _finishSpan(Span span,
                                  TracingConfiguration tracingConfiguration,
                                  Duration executeDuration2,
                                  ConnectionInfo connectionInfo,
                                  long threadId,
                                  String threadName) {
    if (isSlowQueryEnabled(tracingConfiguration)) {
      Duration executeDuration = executeDuration2;
      if (null != executeDuration && executeDuration.toMillis() > tracingConfiguration.getSlowQueryThresholdMs()) {
        TAG_SLOW.set(span, true);
      }
    }
    TAG_CONNECTION_ID.set(span, connectionInfo.getConnectionId());
    TAG_THREAD_ID.set(span, String.valueOf(threadId));
    TAG_THREAD_NAME.set(span, threadName);
    span.finish();
  }

  private static boolean isSlowQueryEnabled(TracingConfiguration tracingConfiguration) {
    long slowQueryThresholdMs = tracingConfiguration.getSlowQueryThresholdMs();
    return slowQueryThresholdMs > 0;
  }

  public static void finishSpan(Span span,
                                MethodExecutionInfo methodExecutionInfo,
                                TracingConfiguration tracingConfiguration) {
    _finishSpan(span,
        tracingConfiguration,
        methodExecutionInfo.getExecuteDuration(),
        methodExecutionInfo.getConnectionInfo(),
        methodExecutionInfo.getThreadId(),
        methodExecutionInfo.getThreadName());
  }

  public static Span buildSpan(String operationName,
                               MethodExecutionInfo methodExecutionInfo,
                               Tracer tracer,
                               TracingConfiguration tracingConfiguration) {
    if (!isTracingEnabled(tracingConfiguration, null)) {
      return NoopSpan.INSTANCE;
    }
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(operationName)
        .asChildOf(tracer.activeSpan())
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

    Span span = spanBuilder.start();
    decorate(span, methodExecutionInfo);
    return span;
  }

  private static boolean isTracingEnabled(TracingConfiguration tracingConfiguration,
                                          String sql) {
    if (!tracingConfiguration.isTraceEnabled()) {
      return false;
    } else if (tracingConfiguration.getIgnoreStatements() != null && tracingConfiguration.getIgnoreStatements().contains(sql)) {
      return false;
    }
    return true;
  }

  public static Span buildSpan(String operationName,
                               String sql,
                               QueryExecutionInfo queryExecutionInfo,
                               Tracer tracer,
                               TracingConfiguration tracingConfiguration) {
    if (!isTracingEnabled(tracingConfiguration, sql)) {
      return NoopSpan.INSTANCE;
    }

    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(operationName)
        .asChildOf(tracer.activeSpan())
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

    Span span = spanBuilder.start();
    decorate(span, sql, queryExecutionInfo);
    return span;
  }

  private static void decorate(Span span,
                               MethodExecutionInfo methodExecutionInfo) {
    Tags.COMPONENT.set(span, COMPONENT_NAME);

    ConnectionInfo connectionInfo = methodExecutionInfo.getConnectionInfo();
    if (null != connectionInfo) {
      String connectionId = connectionInfo.getConnectionId();
      TAG_CONNECTION_ID.set(span, connectionId);
    }
    ConnectionMetadata metadata = Optional.of(methodExecutionInfo)
        .map(info -> info.getConnectionInfo())
        .map(info -> info.getOriginalConnection())
        .map(conn -> conn.getMetadata())
        .orElse(null);
    tagDBType(span, metadata);
    TAG_CONNECTION_CREATE_THREAD_ID.set(span, String.valueOf(methodExecutionInfo.getThreadId()));
    TAG_CONNECTION_CREATE_THREAD_NAME.set(span, methodExecutionInfo.getThreadName());
  }

  private static void decorate(Span span, String sql,
                               QueryExecutionInfo queryExecutionInfo) {
    Tags.COMPONENT.set(span, COMPONENT_NAME);

    if (isNotEmpty(sql)) {
      Tags.DB_STATEMENT.set(span, sql);
    }

    String connectionId = queryExecutionInfo.getConnectionInfo().getConnectionId();
    ConnectionMetadata metadata = Optional.of(queryExecutionInfo)
        .map(info -> info.getConnectionInfo())
        .map(info -> info.getOriginalConnection())
        .map(conn -> conn.getMetadata())
        .orElse(null);
    tagDBType(span, metadata);
    TAG_CONNECTION_ID.set(span, connectionId);
    TAG_QUERY_TYPE.set(span, queryExecutionInfo.getType().toString());
    if (ExecutionType.BATCH == queryExecutionInfo.getType()) {
      TAG_BATCH_SIZE.set(span, queryExecutionInfo.getBatchSize());
    }
  }

  private static void tagDBType(Span span, ConnectionMetadata metadata) {
    if (null != metadata) {
      String databaseProductName = metadata.getDatabaseProductName();
      String databaseVersion = metadata.getDatabaseVersion();
      Tags.DB_TYPE.set(span, String.join(databaseProductName, ":", databaseVersion));
    }
  }

  public static void onError(Throwable throwable,
                             Span span) {
    Tags.ERROR.set(span, Boolean.TRUE);

    if (throwable != null) {
      span.log(errorLogs(throwable));
    }
    span.finish();
  }

  private static Map<String, Object> errorLogs(Throwable throwable) {
    Map<String, Object> errorLogs = new HashMap<>(3);
    errorLogs.put("event", Tags.ERROR.getKey());
    errorLogs.put("error.object", throwable);
    return errorLogs;
  }

  private static boolean isNotEmpty(CharSequence s) {
    return s != null && !"".contentEquals(s);
  }
}
