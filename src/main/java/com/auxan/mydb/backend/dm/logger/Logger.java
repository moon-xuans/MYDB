package com.auxan.mydb.backend.dm.logger;

/**
 * 日志的接口
 * @author axuan
 */
public interface Logger {

  /**创建日志*/
  void log(byte[] log);

  void truncate(long x) throws Exception;

  /**获取到下一个日志*/
  byte[] next();

  /**倒带*/
  void rewind();

  void close();
}
