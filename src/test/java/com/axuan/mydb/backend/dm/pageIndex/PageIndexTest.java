package com.axuan.mydb.backend.dm.pageIndex;

import com.axuan.mydb.backend.dm.pageCache.PageCache;
import org.junit.Test;

/**
 * @author axuan
 * @date 2022/5/21
 **/
public class PageIndexTest {

  @Test
  public void testPageIndex() {
    PageIndex pIndex = new PageIndex();
    int threshold = PageCache.PAGE_SIZE / 20;
    for (int i = 0; i < 20; i++) {
      pIndex.add(i, i * threshold);
      pIndex.add(i, i * threshold);
      pIndex.add(i, i * threshold);
    }

    for (int k = 0; k < 3; k++) {
      for (int i = 0; i < 19; i++) {
        PageInfo pi = pIndex.select(i * threshold);
        assert pi != null;
        assert pi.pgNo == i + 1;
      }
    }
  }

}
