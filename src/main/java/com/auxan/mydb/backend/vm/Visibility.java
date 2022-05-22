package com.auxan.mydb.backend.vm;

import com.auxan.mydb.backend.tm.TransactionManager;

/**
 * 用来判断数据可见性的
 * @author axuan
 * @date 2022/5/22
 **/
public class Visibility {


  // 版本跳跃指的是，不能读到已提交比自己大的xid，也不能读到当时活跃的xid
  public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
    long xmax = e.getXmax();
    if (t.level == 0) {
      return false; // 如果是读已提交的情况下，则会忽略版本跳跃的问题，直接返回false即可
    } else {
      return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
    }
  }


  public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
    if (t.level == 0) {
      return readCommitted(tm, t, e);
    } else {
      return repeatableRead(tm, t, e);
    }
  }

  private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
    long xid = t.xid;
    long xmin = e.getXmin();
    long xmax = e.getXmax();
    if (xmin == xid && xmax == 0) return true;

    if (tm.isCommitted(xmin)) {
      if (xmax == 0) return true;
      if (xmax != xid) {
        if (!tm.isCommitted(xmax)) {
          return true;
        }
      }
    }
    return false;
  }


  private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
    long xid = t.xid;
    long xmin = e.getXmin();
    long xmax = e.getXmax();
    if (xmin == xid && xmax == 0) return true;

    if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
      if (xmax == 0) return true;
      if (xmax != xid) {
        if (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
          return true;
        }
      }
    }
    return false;
  }
}
