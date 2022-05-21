package com.auxan.mydb.backend.dm.pageCache;

import com.auxan.mydb.backend.dm.page.Page;
import com.auxan.mydb.backend.dm.pageCache.impl.PageCacheImpl;
import com.auxan.mydb.backend.utils.Panic;
import com.auxan.mydb.common.Error;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 页面缓存
 * @author axuan
 */
public interface PageCache {

  public static final int PAGE_SIZE = 1 << 13;  // 默认页面大小为8k

  /**新建页面*/
  int newPage(byte[] initData);

  /**根据页号获取页面*/
  Page getPage(int pgNo) throws Exception;

  /**关闭页面*/
  void close();

  /**释放页面*/
  void release(Page page);


  /**设置最大页，用于截断文件*/
  void truncateByPgNo(int maxPgNo);

  /**获取页面数量*/
  int getPageNumber();

  /**刷回Page*/
  void flushPage(Page pg);

  public static PageCacheImpl create(String path, long memory) {
    File f = new File(path + PageCacheImpl.DB_SUFFIX);
    try {
      if (!f.createNewFile()) {
        Panic.panic(Error.FileExistsException);
      }
    } catch (Exception e) {
      Panic.panic(e);
    }
    if (!f.canRead() || !f.canWrite()) {
      Panic.panic(Error.FileCannotRWException);
    }

    FileChannel fc = null;
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(f, "rw");
      fc = raf.getChannel();
    } catch (FileNotFoundException e) {
      Panic.panic(e);
    }
    return new PageCacheImpl(raf, fc, (int)memory / PAGE_SIZE);
  }


  public static PageCacheImpl open(String path, long memory) {
    File f = new File(path + PageCacheImpl.DB_SUFFIX);
    if (!f.exists()) {
      Panic.panic(Error.FileNotExistsException);
    }
    if (!f.canWrite() || !f.canWrite()) {
      Panic.panic(Error.FileCannotRWException);
    }

    FileChannel fc = null;
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(f, "rw");
      fc = raf.getChannel();
    } catch (FileNotFoundException e) {
      Panic.panic(e);
    }
    return new PageCacheImpl(raf, fc, (int)memory / PAGE_SIZE);
  }
}

