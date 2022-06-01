package com.axuan.mydb.backend.vm;

import com.axuan.mydb.backend.dm.DataManager;
import com.axuan.mydb.backend.tm.TransactionManager;
import com.axuan.mydb.backend.vm.impl.VersionManagerImpl;

/**
 * 用来做版本控制的
 * @author axuan
 * @date 2022/5/21
 **/
public interface VersionManager {
  byte[] read(long xid, long uid) throws Exception;
  long insert(long xid, byte[] data) throws Exception;
  boolean delete(long xid, long uid) throws Exception;

  long begin(int level);
  void commit(long xid) throws Exception;
  void abort(long xid);

  public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
    return new VersionManagerImpl(tm, dm);
  }
}
