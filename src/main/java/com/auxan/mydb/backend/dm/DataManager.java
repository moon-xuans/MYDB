package com.auxan.mydb.backend.dm;

import com.auxan.mydb.backend.dm.dataItem.DataItem;
import com.auxan.mydb.backend.dm.pageCache.PageCache;
import com.auxan.mydb.backend.tm.TransactionManager;

/**
 * dm只向上提供读和插入操作，修改操作游dataItem实现
 * @author axuan
 */
public interface DataManager {
  DataItem read(long uid) throws Exception;

  long insert(long xid, byte[] data) throws Exception;

  void close();


  public static DataManager create(String path, long mem, TransactionManager tm) {
    return null;
  }

}
