package com.axuan.mydb.common;

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

  // tbm
  public static Exception DuplicationTableException = new RuntimeException("Duplicated table!");
  public static Exception TableNotFoundException = new RuntimeException("Table not found!");
  public static Exception InvalidValuesException = new RuntimeException("Invalid values!");
  public static Exception FieldNotIndexedException = new RuntimeException("Field not indexed!");
  public static Exception InvalidLogOpException = new RuntimeException("Invalid logic operation!");
  public static Exception FieldNotFoundException = new RuntimeException("Field not found!");
  // parser
  public static Exception InvalidCommandException = new RuntimeException("Invalid command!");
  public static Exception TableNoIndexException = new RuntimeException("Table has no index!");

  // transport
  public static Exception InvalidPkgDataException = new RuntimeException("Invalid package data!");

  // server
  public static Exception NestedTransactionException = new RuntimeException("Nested transaction not supported!");
  public static Exception NoTransactionException = new RuntimeException("Not in transaction!");

  // launcher
  public static Exception InvalidMemException = new RuntimeException("Invalid memory!");
}
