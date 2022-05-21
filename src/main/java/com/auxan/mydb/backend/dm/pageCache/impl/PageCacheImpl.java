package com.auxan.mydb.backend.dm.pageCache.impl;

import com.auxan.mydb.backend.dm.page.Page;
import com.auxan.mydb.backend.dm.page.impl.PageImpl;
import com.auxan.mydb.backend.dm.pageCache.PageCache;
import com.auxan.mydb.backend.utils.Panic;
import com.auxan.mydb.common.AbstractCache;
import com.auxan.mydb.common.Error;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面缓存的具体实现类
 * PageCache是中间桥梁，对文件系统进行读写，并向上提供服务
 * @author axuan
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

  /**页面缓存的最小页数*/
  private static final int MEM_MIN_LIM = 10;

  public static final String DB_SUFFIX = ".db";


  private RandomAccessFile file;

  private FileChannel fc;

  private Lock fileLock;


  private AtomicInteger pageNumbers;

  public PageCacheImpl(RandomAccessFile file, FileChannel fc, int maxResources) {
    super(maxResources);
    if (maxResources < MEM_MIN_LIM) {
      Panic.panic(Error.MemTooSmallException);
    }
    long length = 0;
    try {
      length = file.length();
    } catch (IOException e) {
      Panic.panic(e);
    }
    this.file = file;
    this.fc = fc;
    this.fileLock = new ReentrantLock();
    this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
  }

  @Override
  public int newPage(byte[] initData) {
    int pgNo = pageNumbers.incrementAndGet();
    Page pg = new PageImpl(pgNo, initData, null);
    // 创建一个新的页面后，立即刷新到文件
    flush(pg);
    return pgNo;
  }

  /**
   * 用于将页面刷新到文件磁盘
   * @param pg
   */
  private void flush(Page pg) {
    int pgNo = pg.getPageNumber();
    long offset = pageOffset(pgNo);

    fileLock.lock();
    try {
      ByteBuffer buf = ByteBuffer.wrap(pg.getData());
      fc.position(offset);
      fc.write(buf);
      fc.force(false);
    } catch (IOException e) {
      Panic.panic(e);
    } finally {
      fileLock.unlock();
    }
  }


  @Override
  public Page getPage(int pgNo) throws Exception {
    return get((long)pgNo);
  }


  @Override
  public void close() {
    super.close();
    try {
      fc.close();
      file.close();
    } catch (IOException e) {
      Panic.panic(e);
    }
  }

  @Override
  public void release(Page page) {
    release((long)page.getPageNumber());
  }

  @Override
  public void truncateByPgNo(int maxPgNo) {
    long size = pageOffset(maxPgNo + 1);
    try {
      file.setLength(size);
    } catch (IOException e) {
      Panic.panic(e);
    }
    pageNumbers.set(maxPgNo);
  }

  @Override
  public int getPageNumber() {
    return pageNumbers.intValue();
  }

  @Override
  public void flushPage(Page pg) {
    flush(pg);
  }

  /**
   * 将脏页刷新到磁盘中
   * @param pg
   */
  @Override
  protected void releaseForCache(Page pg) {
    if (pg.isDirty()) {
      flush(pg);
      pg.setDirty(false);
    }
  }

  /**
   * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
   * @param key
   * @return
   * @throws Exception
   */
  @Override
  protected Page getForCache(long key) throws Exception {
    int pgNo = (int)key;
    long offset = PageCacheImpl.pageOffset(key);

    ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
    fileLock.lock();
    try {
      fc.position(offset);
      fc.read(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    fileLock.unlock();
    return new PageImpl(pgNo, buf.array(), this);
  }

  /**
   * 通过页号计算出在文件中的偏移量
   * @param pgNo
   * @return
   */
  private static long pageOffset(long pgNo) {
    return (pgNo - 1) * PAGE_SIZE;
  }
}
