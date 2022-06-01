package com.axuan.mydb.backend.dm.page;

/**
 * 页面接口
 * @author axuan
 */
public interface Page {

  /**页面加锁*/
  void lock();

  /**页面解锁*/
  void unlock();

  /**释放页面*/
  void release();

  /**设置为脏页*/
  void setDirty(boolean dirty);

  /**判断是否脏页*/
  boolean isDirty();

  /**获得页号*/
  int getPageNumber();

  /**获得页面数据*/
  byte[] getData();
}
