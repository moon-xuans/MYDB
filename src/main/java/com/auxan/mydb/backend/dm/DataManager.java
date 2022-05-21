package com.auxan.mydb.backend.dm;

import com.auxan.mydb.backend.dm.dataItem.DataItem;
import com.auxan.mydb.backend.dm.logger.Logger;
import com.auxan.mydb.backend.dm.page.PageOne;
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
    PageCache pc = PageCache.create(path, mem);
    Logger lg = Logger.create(path);

    DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
    dm.initPageOne();
    return dm;
  }

  public static DataManager open(String path, long mem, TransactionManager tm) {
    PageCache pc = PageCache.open(path, mem);
    Logger lg = Logger.open(path);
    DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
    if (!dm.loadCheckPageOne()) {
      Recover.recover(tm, lg, pc);
    }
    dm.fillPageIndex();
    PageOne.setVcOpen(dm.pageOne);
    dm.pc.flushPage(dm.pageOne);

    return dm;
  }

}
