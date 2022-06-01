package com.axuan.mydb.backend.tm;

/**
 * @author axuan
 * @date 2022/5/21
 **/
public class MockTransactionManager implements TransactionManager {

  @Override
  public long begin() {
    return 0;
  }

  @Override
  public void commit(long xid) {

  }

  @Override
  public void abort(long xid) {

  }

  @Override
  public boolean isActive(long xid) {
    return false;
  }

  @Override
  public boolean isCommitted(long xid) {
    return false;
  }

  @Override
  public boolean isAborted(long xid) {
    return false;
  }

  @Override
  public void close() {

  }
}
