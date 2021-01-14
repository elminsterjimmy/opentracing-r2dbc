package opentracing.r2dbc.common;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.BooleanTag;
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

public class SpanUtils {

  private static final BooleanTag SLOW = new BooleanTag("slow");
  private static final String COMPONENT_NAME = "opentracing-r2dbc";
  private static final String TAG_CONNECTION_ID = "connectionId";
  private static final String TAG_CONNECTION_CREATE_THREAD_ID = "threadIdOnCreate";
  private static final String TAG_CONNECTION_CREATE_THREAD_NAME = "threadNameOnCreate";
  private static final String TAG_BATCH_SIZE = "batchSize";
  private static final String TAG_QUERY_TYPE = "type";

  public static void finishSpan(Span span,
                                QueryExecutionInfo queryExecutionInfo,
                                TracingConfiguration tracingConfiguration) {
    if (isSlowQueryEnabled(tracingConfiguration)) {
      Duration executeDuration = queryExecutionInfo.getExecuteDuration();
      if (null != executeDuration && executeDuration.toMillis() > tracingConfiguration.getSlowQueryThresholdMs()) {
        SLOW.set(span, true);
      }
    }
    span.finish();
  }

  private static boolean isSlowQueryEnabled(TracingConfiguration tracingConfiguration) {
    long slowQueryThresholdMs = tracingConfiguration.getSlowQueryThresholdMs();
    return slowQueryThresholdMs > 0;
  }

  public static void finishSpan(Span span,
                                MethodExecutionInfo methodExecutionInfo,
                                TracingConfiguration tracingConfiguration) {
    if (isSlowQueryEnabled(tracingConfiguration)) {
      Duration executeDuration = methodExecutionInfo.getExecuteDuration();
      if (null != executeDuration && executeDuration.toMillis() > tracingConfiguration.getSlowQueryThresholdMs()) {
        SLOW.set(span, true);
      }
    }
    span.finish();
  }

  public static Span buildSpan(String operationName,
                                MethodExecutionInfo methodExecutionInfo,
                                Tracer tracer,
                                TracingConfiguration tracingConfiguration) {
    if (!isTracingEnabled(tracingConfiguration, null)) {
      return NoopSpan.INSTANCE;
    }

    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(operationName)
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
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

    Span span = spanBuilder.start();
    decorate(span, sql, queryExecutionInfo);
    return span;
  }

  private static void decorate(Span span,
                               MethodExecutionInfo methodExecutionInfo) {
    Tags.COMPONENT.set(span, COMPONENT_NAME);

    ConnectionInfo connectionInfo = methodExecutionInfo.getConnectionInfo();
    String connectionId = connectionInfo.getConnectionId();
    ConnectionMetadata metadata = Optional.of(methodExecutionInfo)
        .map(info -> info.getConnectionInfo())
        .map(info -> info.getOriginalConnection())
        .map(conn -> conn.getMetadata())
        .orElse(null);
    tagDBType(span, metadata);
    span.setTag(TAG_CONNECTION_ID, connectionId);
    span.setTag(TAG_CONNECTION_CREATE_THREAD_ID, String.valueOf(methodExecutionInfo.getThreadId()));
    span.setTag(TAG_CONNECTION_CREATE_THREAD_NAME, methodExecutionInfo.getThreadName());
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
    span.setTag(TAG_CONNECTION_ID, connectionId);
    span.setTag(TAG_QUERY_TYPE, queryExecutionInfo.getType().toString());
    if (ExecutionType.BATCH == queryExecutionInfo.getType()) {
      span.setTag(TAG_BATCH_SIZE, Integer.toString(queryExecutionInfo.getBatchSize()));
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
