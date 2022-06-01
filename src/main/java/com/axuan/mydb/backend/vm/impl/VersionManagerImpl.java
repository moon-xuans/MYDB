package com.axuan.mydb.backend.vm.impl;

import com.axuan.mydb.backend.dm.DataManager;
import com.axuan.mydb.backend.tm.TransactionManager;
import com.axuan.mydb.backend.tm.impl.TransactionManagerImpl;
import com.axuan.mydb.backend.utils.Panic;
import com.axuan.mydb.backend.vm.Entry;
import com.axuan.mydb.backend.vm.LockTable;
import com.axuan.mydb.backend.vm.Transaction;
import com.axuan.mydb.backend.vm.VersionManager;
import com.axuan.mydb.backend.vm.Visibility;
import com.axuan.mydb.backend.common.AbstractCache;
import com.axuan.mydb.common.Error;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author axuan
 * @date 2022/5/21
 **/
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

  public TransactionManager tm;
  public DataManager dm;
  public Map<Long, Transaction> activeTransaction;  // 记录此时active的事务
  public Lock lock;
  public LockTable lt;


  public VersionManagerImpl(TransactionManager tm, DataManager dm) {
    super(0);
    this.tm = tm;
    this.dm = dm;
    this.activeTransaction = new HashMap<>();
    activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
    this.lock = new ReentrantLock();
    this.lt = new LockTable();
  }

  @Override
  public byte[] read(long xid, long uid) throws Exception {
    lock.lock();
    Transaction t = activeTransaction.get(xid); // 读的时候先通过xid获取该事务
    lock.unlock();
    if (t.err != null) {
      throw t.err;
    }
    Entry entry = super.get(uid);  // 通过缓存获得entry
    try {
      if (Visibility.isVisible(tm,t, entry)) {
        return entry.data();
      }else{
        return null;
      }
    } finally {
      entry.release();
    }
  }

  @Override
  public long insert(long xid, byte[] data) throws Exception {
    lock.lock();
    Transaction t = activeTransaction.get(xid);
    lock.unlock();
    if (t.err != null) {
      throw t.err;
    }
    byte[] raw = Entry.wrapEntryRaw(xid, data);
    return dm.insert(xid, raw);
  }

  @Override
  public boolean delete(long xid, long uid) throws Exception {
    lock.lock();
    Transaction t = activeTransaction.get(xid); // 获得其事务
    lock.unlock();

    if (t.err != null) {
      throw t.err;
    }
    Entry entry = super.get(uid);
    try {
      if (!Visibility.isVisible(tm, t, entry)) { // 如果不满足可见性，则直接返回false
        return false;
      }
      Lock l = null;
      try {
        l = lt.add(xid, uid); // 如果xid持有uid失败，返回lock，则进行加锁阻塞，等待其释放
      } catch (Exception e) {
        t.err = Error.ConcurrentUpdateException;
        internAbort(xid, true);
        t.autoAborted = true;
        throw t.err;
      }
      if (l != null) {
        l.lock();
        l.unlock();
      }

      if (entry.getXmax() == xid) { // 如果xmax就是它本身，则重复操作，返回false
        return false;
      }

      if (Visibility.isVersionSkip(tm, t, entry)) { // 如果存在版本跳跃
        t.err = Error.ConcurrentUpdateException;
        internAbort(xid, true); // 则要自己回滚事务
        t.autoAborted = true;
        throw t.err;
      }

      entry.setMax(xid);
      return true;
    } finally {
      entry.release();
    }
  }

  @Override
  public long begin(int level) {
    lock.lock();
    try {
      long xid = tm.begin();
      Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
      activeTransaction.put(xid, t);
      return xid;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void commit(long xid) throws Exception {
    lock.lock();
    Transaction t = activeTransaction.get(xid);  // 通过xid获取这个事务
    lock.unlock();

    try {
      if (t.err != null) {
        throw t.err;
      }
    } catch (NullPointerException n) {
      System.out.println(xid);
      System.out.println(activeTransaction.keySet());
      Panic.panic(n);
    }

    lock.lock();
    activeTransaction.remove(xid); // 并从active事务中移除掉
    lock.unlock();

    lt.remove(xid);  // 既然这个事务已经提交，则去掉在locktable中的关联
    tm.commit(xid); // 通过tm提交这个事务
  }

  @Override
  public void abort(long xid) {
    internAbort(xid, false);
  }

  // abort的事务有两种，手动和自动。
  // 手动指的是调用abort()方法
  // 而自动，则是在事务被检测出出现死锁时，会自动撤销回滚事务，或者出现版本跳跃时，也会自动回滚
  private void internAbort(long xid, boolean autoAborted) {
    lock.lock();
    Transaction t = activeTransaction.get(xid);
    if (!autoAborted) {
      activeTransaction.remove(xid);
    }
    lock.unlock();

    if (t.autoAborted) return;
    lt.remove(xid);  // 手动的话，则需要解除locktable的一些关系
    tm.abort(xid); // 并要通过tm进行abort
  }

  public void releaseEntry(Entry entry) {
    super.release(entry.getUid());
  }



  @Override
  protected void releaseForCache(Entry entry) {
    entry.remove();
  }

  @Override
  protected Entry getForCache(long uid) throws Exception {
    Entry entry = Entry.loadEntry(this, uid);
    if (entry == null) {
      throw Error.NullEntryException;
    }
    return entry;
  }
}
