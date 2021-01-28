package opentracing.r2dbc.common;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.r2dbc.proxy.core.ConnectionInfo;
import io.r2dbc.proxy.core.MethodExecutionInfo;
import io.r2dbc.proxy.core.QueryExecutionInfo;
import io.r2dbc.proxy.core.QueryInfo;
import io.r2dbc.proxy.listener.ProxyMethodExecutionListener;

import static java.util.stream.Collectors.joining;
import static opentracing.r2dbc.common.TagConstants.*;

public class OpenTracingExecutorListener implements ProxyMethodExecutionListener {

  static final String CONNECTION_SPAN_KEY = "connectionSpan";
  static final String TRANSACTION_SPAN_KEY = "transactionSpan";
  static final String QUERY_SPAN_KEY = "querySpan";
  public static final String ANNOTATION_TRANSACTION_ROLLBACK_TO_SAVEPOINT = "Transaction rollback to savepoint";
  public static final String ANNOTATION_ROLLBACK_TO_SAVEPOINT = "Rollback to savepoint";
  public static final String ANNOTATION_TRANSACTION_ROLLBACK = "Transaction rollback";
  public static final String ANNOTATION_ROLLBACK = "Rollback";
  public static final String ANNOTATION_TRANSACTION_COMMIT = "Transaction commit";
  public static final String ANNOTATION_COMMIT = "Commit";
  public static final String SPAN_NAME_R2DBC_TRANSACTION = "r2dbc:transaction";
  public static final String SPAN_NAME_R2DBC_QUERY = "r2dbc:query";
  public static final String ANNOTATION_CONNECTION_CREATED = "Connection created";
  public static final String INITIAL_CONNECTION_SPAN_KEY = "initialConnectionSpan";
  public static final String SPAN_NAME_R2DBC_CONNECTION = "r2dbc:connection";

  private final Tracer tracer;
  private final TracingConfiguration tracingConfiguration;

  public OpenTracingExecutorListener(Tracer tracer, TracingConfiguration tracingConfiguration) {
    this.tracer = tracer;
    this.tracingConfiguration = tracingConfiguration;
  }

  @Override
  public void beforeCreateOnConnectionFactory(MethodExecutionInfo methodExecutionInfo) {
    Span connectionSpan = SpanUtils.buildSpan(SPAN_NAME_R2DBC_CONNECTION, methodExecutionInfo,
        tracer, tracingConfiguration);
    methodExecutionInfo.getValueStore().put(INITIAL_CONNECTION_SPAN_KEY, connectionSpan);
  }

  @Override
  public void afterCreateOnConnectionFactory(MethodExecutionInfo methodExecutionInfo) {
    Span connectionSpan = methodExecutionInfo.getValueStore().get(INITIAL_CONNECTION_SPAN_KEY, Span.class);
    Throwable thrown = methodExecutionInfo.getThrown();
    if (thrown != null) {
      SpanUtils.onError(thrown, connectionSpan);
      return;
    }

    ConnectionInfo connectionInfo = methodExecutionInfo.getConnectionInfo();
    TAG_ANNOTATION.set(connectionSpan, ANNOTATION_CONNECTION_CREATED);
    connectionInfo.getValueStore().put(CONNECTION_SPAN_KEY, connectionSpan);
  }

  @Override
  public void afterCloseOnConnection(MethodExecutionInfo methodExecutionInfo) {
    ConnectionInfo connectionInfo = methodExecutionInfo.getConnectionInfo();
    String connectionId = connectionInfo.getConnectionId();
    Span connectionSpan = connectionInfo.getValueStore().get(CONNECTION_SPAN_KEY, Span.class);
    if (connectionSpan == null) {
      return;
    }

    TAG_CONNECTION_ID.set(connectionSpan, connectionId);
    TAG_CONNECTION_CLOSE_THREAD_ID.set(connectionSpan, String.valueOf(methodExecutionInfo.getThreadId()));
    TAG_CONNECTION_CLOSE_THREAD_NAME.set(connectionSpan, methodExecutionInfo.getThreadName());
    TAG_TRANSACTION_COUNT.set(connectionSpan, connectionInfo.getTransactionCount());
    TAG_COMMIT_COUNT.set(connectionSpan, connectionInfo.getCommitCount());
    TAG_ROLLBACK_COUNT.set(connectionSpan, connectionInfo.getRollbackCount());

    Throwable thrown = methodExecutionInfo.getThrown();
    if (thrown != null) {
      SpanUtils.onError(thrown, connectionSpan);
    } else {
      SpanUtils.finishSpan(connectionSpan, methodExecutionInfo, tracingConfiguration);
    }
  }

  @Override
  public void beforeQuery(QueryExecutionInfo queryExecutionInfo) {
    String queries = queryExecutionInfo.getQueries().stream()
        .map(QueryInfo::getQuery)
        .collect(joining(", "));
    Span querySpan = SpanUtils.buildSpan(SPAN_NAME_R2DBC_QUERY, queries, queryExecutionInfo,
        tracer, tracingConfiguration);
    queryExecutionInfo.getValueStore().put(QUERY_SPAN_KEY, querySpan);
  }

  @Override
  public void afterQuery(QueryExecutionInfo queryExecutionInfo) {
    Span querySpan = queryExecutionInfo.getValueStore().get(QUERY_SPAN_KEY, Span.class);
    TAG_QUERY_SUCCESS.set(querySpan, queryExecutionInfo.isSuccess());

    Throwable thrown = queryExecutionInfo.getThrowable();
    if (thrown != null) {
      SpanUtils.onError(thrown, querySpan);
    } else {
      TAG_QUERY_MAPPED_RESULT_COUNT.set(querySpan, queryExecutionInfo.getCurrentResultCount());
      SpanUtils.finishSpan(querySpan, queryExecutionInfo, tracingConfiguration);
    }
  }

  @Override
  public void beforeBeginTransactionOnConnection(MethodExecutionInfo methodExecutionInfo) {
    Span transactionSpan = SpanUtils.buildSpan(SPAN_NAME_R2DBC_TRANSACTION, methodExecutionInfo,
        tracer, tracingConfiguration);
    methodExecutionInfo.getConnectionInfo().getValueStore().put(TRANSACTION_SPAN_KEY, transactionSpan);
  }

  @Override
  public void afterCommitTransactionOnConnection(MethodExecutionInfo methodExecutionInfo) {
    afterTransactionOnConnection(methodExecutionInfo, ANNOTATION_COMMIT, ANNOTATION_TRANSACTION_COMMIT);
  }

  private void afterTransactionOnConnection(MethodExecutionInfo methodExecutionInfo,
                                            String transactionAnnotation,
                                            String connectionAnnotation) {
    ConnectionInfo connectionInfo = methodExecutionInfo.getConnectionInfo();
    Span transactionSpan = connectionInfo.getValueStore().get(TRANSACTION_SPAN_KEY, Span.class);
    if (transactionSpan != null) {
      TAG_ANNOTATION.set(transactionSpan, transactionAnnotation);
      SpanUtils.finishSpan(transactionSpan, methodExecutionInfo, tracingConfiguration);
    }
    Span connectionSpan = connectionInfo.getValueStore().get(CONNECTION_SPAN_KEY, Span.class);
    if (connectionSpan != null) {
      TAG_ANNOTATION.set(connectionSpan, connectionAnnotation);
    }
  }

  @Override
  public void afterRollbackTransactionOnConnection(MethodExecutionInfo methodExecutionInfo) {
    afterTransactionOnConnection(methodExecutionInfo, ANNOTATION_ROLLBACK, ANNOTATION_TRANSACTION_ROLLBACK);
  }

  @Override
  public void afterRollbackTransactionToSavepointOnConnection(MethodExecutionInfo methodExecutionInfo) {
    ConnectionInfo connectionInfo = methodExecutionInfo.getConnectionInfo();
    String savepoint = (String) methodExecutionInfo.getMethodArgs()[0];

    Span transactionSpan = connectionInfo.getValueStore().get(TRANSACTION_SPAN_KEY, Span.class);
    if (transactionSpan != null) {
      TAG_ANNOTATION.set(transactionSpan, ANNOTATION_ROLLBACK_TO_SAVEPOINT);
      TAG_TRANSACTION_SAVEPOINT.set(transactionSpan, savepoint);
      SpanUtils.finishSpan(transactionSpan, methodExecutionInfo, tracingConfiguration);
    }

    Span connectionSpan = connectionInfo.getValueStore().get(CONNECTION_SPAN_KEY, Span.class);
    TAG_ANNOTATION.set(connectionSpan, ANNOTATION_TRANSACTION_ROLLBACK_TO_SAVEPOINT);
  }
}