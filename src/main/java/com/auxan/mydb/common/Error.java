package com.auxan.mydb.common;

/**
 * 系统中会出现的异常的常量类
 * @author axuan
 */
public class Error {

  // common
  public static final Exception CacheFullException = new RuntimeException("Cache is full!");
  public static final Exception FileExistsException = new RuntimeException("File already exists");
  public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
  public static final Exception FileCannotRWException = new RuntimeException("File cannot read or write");


  // dm
  public static final Exception MemTooSmallException = new RuntimeException("Memory too small");
  public static final Exception DataToolLargeException = new RuntimeException("Data too large!");


  // tm
  public static final Exception BadXidFileException = new RuntimeException("Bad XID file!");



  // vm
  public static final Exception DeadLockException = new RuntimeException("DeadLock!");
  public static final Exception NullEntryException = new RuntimeException("Null entry!");
  public static final Exception ConcurrentUpdateException = new RuntimeException("Concurrent update issue!");

}
