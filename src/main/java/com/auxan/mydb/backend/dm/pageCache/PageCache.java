package com.auxan.mydb.backend.dm.pageCache;

import com.auxan.mydb.backend.dm.page.Page;

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
}

