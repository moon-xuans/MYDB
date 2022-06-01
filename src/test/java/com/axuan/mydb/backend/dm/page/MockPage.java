package com.axuan.mydb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MockPage implements Page {

  private int pgNo;
  private byte[] data;
  private Lock lock = new ReentrantLock();

  public static MockPage newMockPage(int pgNo, byte[] data) {
    MockPage mp = new MockPage();
    mp.pgNo = pgNo;
    mp.data = data;
    return mp;
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

  }

  @Override
  public void setDirty(boolean dirty) {

  }


  @Override
  public boolean isDirty() {
    return false;
  }

  @Override
  public int getPageNumber() {
    return pgNo;
  }

  @Override
  public byte[] getData() {
    return data;
  }
}
