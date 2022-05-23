package com.axuan.mydb.backend.im;

import com.auxan.mydb.backend.dm.DataManager;
import com.auxan.mydb.backend.dm.pageCache.impl.PageCacheImpl;
import com.auxan.mydb.backend.im.BPlusTree;
import com.axuan.mydb.backend.tm.MockTransactionManager;
import java.io.File;
import java.util.List;
import org.junit.Test;

/**
 * @author axuan
 * @date 2022/5/23
 **/
public class BPlusTreeTest {

  @Test
  public void testTreeSingle() throws Exception {
    MockTransactionManager tm = new MockTransactionManager();
    DataManager dm = DataManager.create("/tmp/TestTreeSingle", PageCacheImpl.PAGE_SIZE * 10, tm);

    long root = BPlusTree.create(dm);
    BPlusTree tree = BPlusTree.load(root, dm);

    int lim = 10000;
    for (int i = lim - 1; i >= 0; i--) {
      tree.insert(i, i);
    }

    for (int i = 0; i < lim; i++) {
      List<Long> uids = tree.search(i);
      assert uids.size() == 1;
      assert uids.get(0) == i;
    }

    assert new File("/tmp/TestTreeSingle.db").delete();
    assert new File("/tmp/TestTreeSingle.log").delete();
  }
}
