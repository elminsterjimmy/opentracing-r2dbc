package opentracing.r2dbc.common;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.GlobalTracerTestUtil;
import io.r2dbc.proxy.core.ConnectionInfo;
import io.r2dbc.proxy.core.ExecutionType;
import io.r2dbc.proxy.core.QueryInfo;
import io.r2dbc.proxy.core.ValueStore;
import io.r2dbc.proxy.test.MockConnectionInfo;
import io.r2dbc.proxy.test.MockMethodExecutionInfo;
import io.r2dbc.proxy.test.MockQueryExecutionInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class OpenTracingExecutorListenerTest {

  private static final MockTracer mockTracer = new MockTracer();
  private static final TracingConfiguration tracingConfiguration = new TracingConfiguration();
  private OpenTracingExecutorListener openTracingExecutorListener = new OpenTracingExecutorListener(mockTracer, tracingConfiguration);

  @BeforeClass
  public static void init() {
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(mockTracer);
    tracingConfiguration.setTraceEnabled(true);
  }

  @Before
  public void before() {
    mockTracer.reset();
  }

  @Test
  public void testCreateOnConnectionFactory() {
    ValueStore valueStore = ValueStore.create();
    ConnectionInfo connectionInfo = MockConnectionInfo.builder()
        .connectionId("foo")
        .valueStore(valueStore)
        .build();
    MockMethodExecutionInfo methodExecutionInfo = MockMethodExecutionInfo.builder()
        .connectionInfo(connectionInfo)
        .threadId(10)
        .threadName("thread-name")
        .build();

    openTracingExecutorListener.beforeCreateOnConnectionFactory(methodExecutionInfo);
    openTracingExecutorListener.afterCreateOnConnectionFactory(methodExecutionInfo);
    openTracingExecutorListener.afterCloseOnConnection(methodExecutionInfo);

    List<MockSpan> spans = mockTracer.finishedSpans();
    Assert.assertEquals(1, spans.size());
    Assert.assertEquals("r2dbc:connection", spans.get(0).operationName());
    Assert.assertEquals("foo", spans.get(0).tags().get("connectionId"));
    Assert.assertEquals("thread-name", spans.get(0).tags().get("threadNameOnCreate"));
    Assert.assertEquals("10", spans.get(0).tags().get("threadIdOnCreate"));
    Assert.assertEquals("Connection created", spans.get(0).tags().get("annotation"));
  }

  @Test
  public void testCreateOnConnectionFactoryWithError() {
    Exception error = new RuntimeException();

    ValueStore valueStore = ValueStore.create();
    ConnectionInfo connectionInfo = MockConnectionInfo.builder()
        .connectionId("foo")
        .valueStore(valueStore)
        .build();
    MockMethodExecutionInfo methodExecutionInfo = MockMethodExecutionInfo.builder()
        .connectionInfo(connectionInfo)
        .threadId(10)
        .threadName("thread-name")
        .setThrown(error)
        .build();

    openTracingExecutorListener.beforeCreateOnConnectionFactory(methodExecutionInfo);
    openTracingExecutorListener.afterCreateOnConnectionFactory(methodExecutionInfo);
    List<MockSpan> spans = mockTracer.finishedSpans();
    Assert.assertEquals(1, spans.size());
    Assert.assertEquals("r2dbc:connection", spans.get(0).operationName());
    Assert.assertEquals(error, spans.get(0).logEntries().get(0).fields().get("error.object"));
  }


  @Test
  public void testQuery() {
    ValueStore valueStore = ValueStore.create();
    ConnectionInfo connectionInfo = MockConnectionInfo.builder()
        .connectionId("foo")
        .valueStore(valueStore)
        .build();
    QueryInfo queryInfo = new QueryInfo("SELECT 1");
    MockQueryExecutionInfo queryExecutionInfo = MockQueryExecutionInfo.builder()
        .connectionInfo(connectionInfo)
        .queryInfo(queryInfo)
        .type(ExecutionType.STATEMENT)
        .threadName("thread-name")
        .threadId(300)
        .isSuccess(true)
        .build();

    openTracingExecutorListener.beforeQuery(queryExecutionInfo);
    openTracingExecutorListener.afterQuery(queryExecutionInfo);

    List<MockSpan> spans = mockTracer.finishedSpans();
    Assert.assertEquals(1, spans.size());
    Assert.assertEquals("r2dbc:query", spans.get(0).operationName());
    Assert.assertEquals("SELECT 1", spans.get(0).tags().get("db.statement"));
    Assert.assertEquals("thread-name", spans.get(0).tags().get("threadName"));
    Assert.assertEquals("300", spans.get(0).tags().get("threadId"));
    Assert.assertEquals(true, spans.get(0).tags().get("success"));
  }

  @Test
  public void testTransactionOnConnectionCommit() {
    ValueStore valueStore = ValueStore.create();
    ConnectionInfo connectionInfo = MockConnectionInfo.builder()
        .connectionId("foo")
        .valueStore(valueStore)
        .build();
    MockMethodExecutionInfo methodExecutionInfo = MockMethodExecutionInfo.builder()
        .connectionInfo(connectionInfo)
        .build();

    openTracingExecutorListener.beforeBeginTransactionOnConnection(methodExecutionInfo);
    openTracingExecutorListener.afterCommitTransactionOnConnection(methodExecutionInfo);

    List<MockSpan> spans = mockTracer.finishedSpans();
    Assert.assertEquals(1, spans.size());
    Assert.assertEquals("r2dbc:transaction", spans.get(0).operationName());
    Assert.assertEquals("Commit", spans.get(0).tags().get("annotation"));
  }

  @Test
  public void testTransactionOnConnectionRollback() {
    ValueStore valueStore = ValueStore.create();
    ConnectionInfo connectionInfo = MockConnectionInfo.builder()
        .connectionId("foo")
        .valueStore(valueStore)
        .build();
    MockMethodExecutionInfo methodExecutionInfo = MockMethodExecutionInfo.builder()
        .connectionInfo(connectionInfo)
        .build();

    openTracingExecutorListener.beforeBeginTransactionOnConnection(methodExecutionInfo);
    openTracingExecutorListener.afterRollbackTransactionOnConnection(methodExecutionInfo);

    List<MockSpan> spans = mockTracer.finishedSpans();
    Assert.assertEquals(1, spans.size());
    Assert.assertEquals("r2dbc:transaction", spans.get(0).operationName());
    Assert.assertEquals("Rollback", spans.get(0).tags().get("annotation"));
  }
}
