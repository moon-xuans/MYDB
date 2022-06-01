package com.axuan.mydb.backend.tm;

import java.io.File;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.Test;

/**
 * @author axuan
 * @date 2022/5/19 上午9:15
 */
public class TransactionManagerTest {
  static Random random = new SecureRandom();

  private int transCnt = 0;
  private int noWorkers = 50;
  private int noWorks = 3000;
  private Lock lock = new ReentrantLock();
  private TransactionManager tm;
  private Map<Long, Byte> transMap;
  private CountDownLatch cdl;

  @Test
  public void testMultiThread() {
    tm = TransactionManager.create("/tmp/tm_test");
    transMap = new ConcurrentHashMap<>();
    cdl = new CountDownLatch(noWorkers);
    for (int i = 0; i < noWorkers; i++) {
      Runnable r = () -> worker();
      new Thread(r).run();
    }
    try {
      cdl.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assert new File("/tmp/tm_test.xid").delete();
  }

  private void worker() {
    boolean inTrans = false;
    long transXID = 0;
    for (int i = 0; i < noWorkers; i++) {
      int op = Math.abs(random.nextInt(6));
      if (op == 0) {
        lock.lock();
        if (inTrans == false) {
          long xid = tm.begin();
          transMap.put(xid, (byte)0);
          transCnt++;
          transXID = xid;
          inTrans = true;
        } else {
          int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
          switch (status) {
            case 1:
              tm.commit(transXID);
              break;
            case 2:
              tm.abort(transXID);
              break;
          }
          transMap.put(transXID, (byte)status);
          inTrans = false;
        }
        lock.unlock();
      } else {
        lock.lock();
        if (transCnt > 0) {
          long xid = (long)((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
          byte status = transMap.get(xid);
          boolean ok = false;
          switch (status) {
            case 0:
              ok = tm.isActive(xid);
              break;
            case 1:
              ok = tm.isCommitted(xid);
              break;
            case 2:
              ok = tm.isAborted(xid);
              break;
          }
          assert ok;
        }
        lock.unlock();
      }
    }
    cdl.countDown();
  }

}
