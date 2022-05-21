package com.auxan.mydb.backend.dm.page.impl;

import com.auxan.mydb.backend.dm.page.Page;
import com.auxan.mydb.backend.dm.pageCache.PageCache;
import java.io.Serializable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面接口的实现类
 * @author axuan
 */
public class PageImpl implements Page{


  private int pageNumber;

  private byte[] data;

  private boolean dirty;

  private Lock lock;


  // 这里Page的实现类有一个pageCache的引用，是为了方便使用
  private PageCache pc;


  public PageImpl(int pageNumber, byte[] data, PageCache pc) {
    this.pageNumber = pageNumber;
    this.data = data;
    this.pc = pc;
    lock = new ReentrantLock();
  }

  @Override
  public void lock() {
    lock.lock();
  }

  @Override
  public void unlock() {
    lock.unlock();
  }

  @Override
  public void release() {
    pc.release(this);
  }

  @Override
  public void setDirty(boolean dirty) {
    this.dirty = dirty;
  }

  @Override
  public boolean isDirty() {
    return dirty;
  }

  @Override
  public int getPageNumber() {
    return pageNumber;
  }

  @Override
  public byte[] getData() {
    return data;
  }
}
