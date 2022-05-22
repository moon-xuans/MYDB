package com.auxan.mydb.backend.vm;

import com.auxan.mydb.backend.tm.impl.TransactionManagerImpl;
import java.util.HashMap;
import java.util.Map;

/**
 * vm对一个事务的抽象
 * @author axuan
 * @date 2022/5/21
 **/
public class Transaction {
  public long xid;
  public int level;
  public Map<Long, Boolean> snapshot;
  public Exception err;
  public boolean autoAborted;

  public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
    Transaction t = new Transaction();
    t.xid = xid;
    t.level = level;
    if (level != 0) {
      t.snapshot = new HashMap<>();
      for (Long x : active.keySet()) {
        t.snapshot.put(x, true);
      }
    }
    return t;
  }

  public boolean isInSnapshot(long xid) {
    if (xid == TransactionManagerImpl.SUPER_XID) {  // 如果是超级事务，直接返回false。因为超级事务默认committed
      return false;
    }
    return snapshot.containsKey(xid);
  }

}
