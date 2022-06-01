package com.axuan.mydb.backend.common;

/**
 * @author axuan
 * @date 2022/6/1
 **/
public class MockCache extends AbstractCache<Long> {

  public MockCache() {
    super(50);
  }

  @Override
  protected void releaseForCache(Long obj) {}

  @Override
  protected Long getForCache(long key) throws Exception {
    return key;
  }
}
