package com.auxan.mydb.common;

/**
 * 系统中会出现的异常的常量类
 * @author axuan
 */
public class Error {

  // common
  public static final Exception CacheFullException = new RuntimeException("Cache is full!");


  // tm
  public static final Exception BadXidFileException = new RuntimeException("Bad XID file!");
}
