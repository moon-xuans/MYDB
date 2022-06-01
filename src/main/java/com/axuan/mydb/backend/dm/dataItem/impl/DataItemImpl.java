package com.axuan.mydb.backend.dm.dataItem.impl;

import com.axuan.mydb.backend.dm.DataManagerImpl;
import com.axuan.mydb.backend.dm.dataItem.DataItem;
import com.axuan.mydb.backend.dm.page.Page;
import com.axuan.mydb.backend.common.SubArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * dataItem 结构如下:
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize 2字节，标识Data的长度
 * @author axuan
 */
public class DataItemImpl implements DataItem {

  public static final int OF_VALID = 0;
  public static final int OF_SIZE = 1;
  public static final int OF_DATA = 3;


  private SubArray raw;
  private byte[] oldRaw;  // 旧数据，和普通数据一样，包括ValidFlag/DataSize/Data
  private Lock rLock;
  private Lock wLock;
  private DataManagerImpl dm;
  private long uid;
  private Page pg;

  public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
    this.raw = raw;
    this.oldRaw = oldRaw;
    ReadWriteLock lock = new ReentrantReadWriteLock();
    rLock = lock.readLock();
    wLock = lock.writeLock();
    this.dm = dm;
    this.uid = uid;
    this.pg = pg;
  }


  public boolean isValid() {
    return raw.raw[raw.start + OF_VALID] == (byte)0;
  }



  @Override
  public SubArray data() {
    return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
  }


  @Override
  public void before() {
    wLock.lock();
    pg.setDirty(true);
    System.arraycopy(raw.raw, raw.start, oldRaw,0, oldRaw.length); // 这里的拷贝，我猜测是整体上拷贝，修改一部分，也会全部拷贝
  }

  @Override
  public void unBefore() {
    System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
    wLock.unlock();
  }

  @Override
  public void after(long xid) {
    dm.logDataItem(xid, this);
    wLock.unlock();
  }

  @Override
  public void release() {
    dm.releaseDataItem(this);
  }


  @Override
  public void lock() {
    wLock.lock();
  }

  @Override
  public void unlock() {
    wLock.unlock();
  }

  @Override
  public void rLock() {
    rLock.lock();
  }

  @Override
  public void rUnLock() {
    rLock.unlock();
  }

  @Override
  public Page page() {
    return pg;
  }

  @Override
  public long getUid() {
    return uid;
  }

  @Override
  public byte[] getOldRaw() {
    return oldRaw;
  }

  @Override
  public SubArray getRaw() {
    return raw;
  }
}
