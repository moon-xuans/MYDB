package com.axuan.mydb.backend.dm.pageIndex;

import com.axuan.mydb.backend.dm.pageCache.PageCache;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面索引，通过按页空闲大小分为40个区间，每次需要获取空间进行插入的时候,便于迅速找到合适的页面
 * 计算出需要页面大小向上取整的number，然后这么大的空间没有，则会取更大的空间
 * @author axuan
 */
public class PageIndex {

  // 将一页化成40个空间
  private static final int INTERVALS_NO = 40;
  private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

  private Lock lock;
  private List<PageInfo>[] lists;

  public PageIndex() {
    lock = new ReentrantLock();
    lists = new List[INTERVALS_NO + 1];
    for (int i = 0; i < INTERVALS_NO + 1; i++) {
      lists[i] = new ArrayList<>();
    }
  }

  public void add(int pgNo, int freeSpace) {
    lock.lock();
    try {
      int number = freeSpace / THRESHOLD;
      lists[number].add(new PageInfo(pgNo, freeSpace));
    } finally {
      lock.unlock();
    }
  }

  public PageInfo select(int spaceSize) {
    lock.lock();
    try {
      int number = spaceSize / THRESHOLD;
      if (number < INTERVALS_NO) number++;  // 对计算出的区间向上取整
      while (number <= INTERVALS_NO) {
        if (lists[number].size() == 0) { // 如果计算出的区间大小没有合适的，那末就加，找到更大的区间
          number++;
          continue;
        }
        return lists[number].remove(0); // 找到后，会将整个页面移出，避免并发操作，这里肯定是每个页面只被添加了一次
      }
      return null;
    } finally {
      lock.unlock();
    }
  }

}
