package com.axuan.mydb.backend.dm.pageCache;

import com.auxan.mydb.backend.dm.page.Page;
import com.auxan.mydb.backend.dm.pageCache.PageCache;
import com.axuan.mydb.backend.dm.page.MockPage;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author axuan
 * @date 2022/5/21
 **/
public class MockPageCache implements PageCache {

  private Map<Integer, MockPage> cache = new HashMap<>();
  private Lock lock = new ReentrantLock();
  private AtomicInteger noPages = new AtomicInteger(0);

  @Override
  public int newPage(byte[] initData) {
    lock.lock();
    try {
      int pgNo = noPages.incrementAndGet();
      MockPage pg = MockPage.newMockPage(pgNo, initData);
      cache.put(pgNo, pg);
      return pgNo;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Page getPage(int pgNo) throws Exception {
    lock.lock();
    try {
      return cache.get(pgNo);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {

  }

  @Override
  public void release(Page page) {

  }

  @Override
  public void truncateByPgNo(int maxPgNo) {

  }

  @Override
  public int getPageNumber() {
    return noPages.intValue();
  }

  @Override
  public void flushPage(Page pg) {

  }
}
