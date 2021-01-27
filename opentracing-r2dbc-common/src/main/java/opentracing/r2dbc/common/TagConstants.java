package opentracing.r2dbc.common;

import io.opentracing.tag.BooleanTag;
import io.opentracing.tag.IntTag;
import io.opentracing.tag.StringTag;

public interface
TagConstants {

  BooleanTag TAG_SLOW = new BooleanTag("slow");
  StringTag TAG_CONNECTION_ID = new StringTag("connectionId");
  StringTag TAG_CONNECTION_CREATE_THREAD_ID = new StringTag("threadIdOnCreate");
  StringTag TAG_CONNECTION_CREATE_THREAD_NAME = new StringTag("threadNameOnCreate");
  IntTag TAG_BATCH_SIZE = new IntTag("batchSize");
  StringTag TAG_QUERY_TYPE = new StringTag("type");
  StringTag TAG_ANNOTATION = new StringTag("annotation");
  StringTag TAG_CONNECTION_CLOSE_THREAD_ID = new StringTag("threadIdOnClose");
  StringTag TAG_CONNECTION_CLOSE_THREAD_NAME = new StringTag("threadNameOnClose");
  IntTag TAG_TRANSACTION_COUNT = new IntTag("transactionCount");
  IntTag TAG_COMMIT_COUNT = new IntTag("commitCount");
  IntTag TAG_ROLLBACK_COUNT = new IntTag("rollbackCount");
  StringTag TAG_THREAD_ID = new StringTag("threadId");
  StringTag TAG_THREAD_NAME = new StringTag("threadName");
  StringTag TAG_TRANSACTION_SAVEPOINT = new StringTag("savepoint");
  BooleanTag TAG_QUERY_SUCCESS = new BooleanTag("success");
  IntTag TAG_QUERY_MAPPED_RESULT_COUNT = new IntTag("mappedResultCount");
}
