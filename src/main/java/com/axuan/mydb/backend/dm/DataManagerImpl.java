package com.axuan.mydb.backend.dm;

import com.axuan.mydb.backend.dm.dataItem.DataItem;
import com.axuan.mydb.backend.dm.dataItem.impl.DataItemImpl;
import com.axuan.mydb.backend.dm.logger.Logger;
import com.axuan.mydb.backend.dm.page.Page;
import com.axuan.mydb.backend.dm.page.PageOne;
import com.axuan.mydb.backend.dm.page.PageX;
import com.axuan.mydb.backend.dm.pageCache.PageCache;
import com.axuan.mydb.backend.dm.pageIndex.PageIndex;
import com.axuan.mydb.backend.dm.pageIndex.PageInfo;
import com.axuan.mydb.backend.tm.TransactionManager;
import com.axuan.mydb.backend.utils.Panic;
import com.axuan.mydb.backend.utils.Types;
import com.axuan.mydb.backend.common.AbstractCache;
import com.axuan.mydb.common.Error;

/**
 * @author axuan
 * @date 2022/5/19 下午11:17
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{

  TransactionManager tm;
  PageCache pc;
  Logger logger;
  PageIndex pIndex;
  Page pageOne;

  public DataManagerImpl(
      PageCache pc,
      Logger logger,
      TransactionManager tm
  ) {
    super(0);
    this.tm = tm;
    this.pc = pc;
    this.logger = logger;
    this.pIndex = new PageIndex();
  }

  @Override
  public DataItem read(long uid) throws Exception {
    DataItemImpl di = (DataItemImpl) super.get(uid);
    if (!di.isValid()) {
      di.release();
      return null;
    }
    return di;
  }

  @Override
  public long insert(long xid, byte[] data) throws Exception {
    byte[] raw = DataItem.wrapDataItemRaw(data);
    if (raw.length > PageX.MAX_FREE_SPACE) {
      throw Error.DataToolLargeException;
    }

    PageInfo pi = null;
    for (int i = 0; i < 5; i++) {
      pi = pIndex.select(raw.length);
      if (pi != null) {
        break;
      } else {
        int newPage = pc.newPage(PageX.initRaw());
        pIndex.add(newPage, PageX.MAX_FREE_SPACE);
      }
    }
    if (pi == null) {
      throw Error.DataToolLargeException;
    }

    Page pg = null;
    int freeSpace = 0;
    try {
      pg = pc.getPage(pi.pgNo);
      byte[] log = Recover.insertLog(xid, pg, raw);
      logger.log(log);

      short offset = PageX.insert(pg, raw);

      pg.release();
      return Types.addressToUid(pi.pgNo, offset);

    } finally {
      // 将取出的pg重新插入pIndex
      if (pg != null) {
        pIndex.add(pi.pgNo, PageX.getFreeSpace(pg));
      } else {
        pIndex.add(pi.pgNo, freeSpace);
      }
    }
  }

  @Override
  public void close() {
    super.close();
    logger.close();

    PageOne.setVcClose(pageOne);
    pageOne.release();
    pc.close();
  }


  // 为xid生成update日志
  public void logDataItem(long xid, DataItem di) {
    byte[] log = Recover.updateLog(xid, di);
    logger.log(log);
  }


  public void releaseDataItem(DataItemImpl di) {
    super.release(di.getUid());
  }


  @Override
  protected void releaseForCache(DataItem di) {
    di.page().release();
  }

  @Override
  protected DataItem getForCache(long uid) throws Exception {
    short offset = (short)(uid & ((1L << 16) - 1));
    uid >>>= 32;
    int pgNo = (int)(uid & ((1L << 32) - 1));
    Page pg = pc.getPage(pgNo);
    return DataItem.parseDataItem(pg, offset, this);
  }


  // 在创建文件时初始化pageOne
  void initPageOne() {
    int pgNo = pc.newPage(PageOne.InitRaw());
    assert pgNo == 1;
    try {
      pageOne = pc.getPage(pgNo);
    } catch (Exception e) {
      Panic.panic(e);
    }
    pc.flushPage(pageOne);
  }

  // 在打开已有文件时读入PageOne，并验证正确性
  public boolean loadCheckPageOne() {
    try {
      pageOne = pc.getPage(1);
    } catch (Exception e) {
      Panic.panic(e);
    }
    return PageOne.checkVc(pageOne);
  }



  // 初始化pageIndex
  void fillPageIndex() {
    int pageNumber = pc.getPageNumber();
    for (int i = 2; i <= pageNumber; i++) {
      Page pg = null;
      try {
        pg = pc.getPage(i);
      } catch (Exception e) {
        Panic.panic(e);
      }
      pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
      pg.release();
    }
  }


}
