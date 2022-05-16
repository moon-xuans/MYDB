package com.auxan.mydb.backend.tm;

/**
 * 事务管理接口
 * @author axuan
 */
public interface TransactionManager {
  /**开启事务*/
  long begin();

  /**提交事务*/
  void commit(long xid);

  /**丢弃事务(也可以说是回滚)*/
  void abort(long xid);

  boolean isActive(long xid);

  boolean isCommitted(long xid);

  boolean isAborted(long xid);

  /** 用于关闭文件通道和文件资源*/
  void close();

}
