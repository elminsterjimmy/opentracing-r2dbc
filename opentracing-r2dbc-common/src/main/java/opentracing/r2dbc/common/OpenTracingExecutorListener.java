package opentracing.r2dbc.common;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.r2dbc.proxy.core.ConnectionInfo;
import io.r2dbc.proxy.core.MethodExecutionInfo;
import io.r2dbc.proxy.core.QueryExecutionInfo;
import io.r2dbc.proxy.core.QueryInfo;
import io.r2dbc.proxy.listener.ProxyMethodExecutionListener;

import static java.util.stream.Collectors.joining;

public class OpenTracingExecutorListener implements ProxyMethodExecutionListener {

  private static final String TAG_CONNECTION_ID = "connectionId";
  private static final String TAG_CONNECTION_CLOSE_THREAD_ID = "threadIdOnClose";
  private static final String TAG_CONNECTION_CLOSE_THREAD_NAME = "threadNameOnClose";
  private static final String TAG_THREAD_ID = "threadId";
  private static final String TAG_THREAD_NAME = "threadName";
  private static final String TAG_QUERY_SUCCESS = "success";
  private static final String TAG_QUERY_MAPPED_RESULT_COUNT = "mappedResultCount";
  private static final String TAG_TRANSACTION_SAVEPOINT = "savepoint";
  private static final String TAG_TRANSACTION_COUNT = "transactionCount";
  private static final String TAG_COMMIT_COUNT = "commitCount";
  private static final String TAG_ROLLBACK_COUNT = "rollbackCount";
  private static final String TAG_ANNOTATION = "annotation";

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
    tracer.activateSpan(connectionSpan);

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

    connectionSpan.setTag(TAG_ANNOTATION, ANNOTATION_CONNECTION_CREATED);

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

    connectionSpan.setTag(TAG_CONNECTION_ID, connectionId);
    connectionSpan.setTag(TAG_CONNECTION_CLOSE_THREAD_ID, String.valueOf(methodExecutionInfo.getThreadId()));
    connectionSpan.setTag(TAG_CONNECTION_CLOSE_THREAD_NAME, methodExecutionInfo.getThreadName());
    connectionSpan.setTag(TAG_TRANSACTION_COUNT, String.valueOf(connectionInfo.getTransactionCount()));
    connectionSpan.setTag(TAG_COMMIT_COUNT, String.valueOf(connectionInfo.getCommitCount()));
    connectionSpan.setTag(TAG_ROLLBACK_COUNT, String.valueOf(connectionInfo.getRollbackCount()));

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
    querySpan.setTag(TAG_THREAD_ID, String.valueOf(queryExecutionInfo.getThreadId()));
    querySpan.setTag(TAG_THREAD_NAME, queryExecutionInfo.getThreadName());
    querySpan.setTag(TAG_QUERY_SUCCESS, Boolean.toString(queryExecutionInfo.isSuccess()));

    Throwable thrown = queryExecutionInfo.getThrowable();
    if (thrown != null) {
      SpanUtils.onError(thrown, querySpan);
    } else {
      querySpan.setTag(TAG_QUERY_MAPPED_RESULT_COUNT, Integer.toString(queryExecutionInfo.getCurrentResultCount()));
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
      transactionSpan.setTag(TAG_ANNOTATION, transactionAnnotation);
      transactionSpan.setTag(TAG_CONNECTION_ID, connectionInfo.getConnectionId());
      transactionSpan.setTag(TAG_THREAD_ID, String.valueOf(methodExecutionInfo.getThreadId()));
      transactionSpan.setTag(TAG_THREAD_NAME, methodExecutionInfo.getThreadName());
      SpanUtils.finishSpan(transactionSpan, methodExecutionInfo, tracingConfiguration);
    }
    Span connectionSpan = connectionInfo.getValueStore().get(CONNECTION_SPAN_KEY, Span.class);
    if (connectionSpan != null) {
      connectionSpan.setTag(TAG_ANNOTATION, connectionAnnotation);
    }
  }

  @Override
  public void afterRollbackTransactionOnConnection(MethodExecutionInfo methodExecutionInfo) {
    afterTransactionOnConnection(methodExecutionInfo, ANNOTATION_ROLLBACK, ANNOTATION_TRANSACTION_ROLLBACK);
  }

  @Override
  public void afterRollbackTransactionToSavepointOnConnection(MethodExecutionInfo methodExecutionInfo) {
    ConnectionInfo connectionInfo = methodExecutionInfo.getConnectionInfo();
    String connectionId = connectionInfo.getConnectionId();
    String savepoint = (String) methodExecutionInfo.getMethodArgs()[0];

    Span transactionSpan = connectionInfo.getValueStore().get(TRANSACTION_SPAN_KEY, Span.class);
    if (transactionSpan != null) {
      transactionSpan.setTag(TAG_ANNOTATION, ANNOTATION_ROLLBACK_TO_SAVEPOINT);
      transactionSpan.setTag(TAG_TRANSACTION_SAVEPOINT, savepoint);
      transactionSpan.setTag(TAG_CONNECTION_ID, connectionId);
      transactionSpan.setTag(TAG_THREAD_ID, String.valueOf(methodExecutionInfo.getThreadId()));
      transactionSpan.setTag(TAG_THREAD_NAME, methodExecutionInfo.getThreadName());
      SpanUtils.finishSpan(transactionSpan, methodExecutionInfo, tracingConfiguration);
    }

    Span connectionSpan = connectionInfo.getValueStore().get(CONNECTION_SPAN_KEY, Span.class);
    connectionSpan.setTag(TAG_ANNOTATION, ANNOTATION_TRANSACTION_ROLLBACK_TO_SAVEPOINT);
  }
}