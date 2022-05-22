package com.auxan.mydb.backend.vm;

import com.auxan.mydb.backend.utils.Parser;
import com.auxan.mydb.common.Error;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 * @author axuan
 * @date 2022/5/21
 **/
public class LockTable {

  private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
  private Map<Long, Long> u2x;        // UID被某个XID持有
  private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
  private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
  private Map<Long, Long> waitU;      // XID正在等待的UID
  private Lock lock;


  public LockTable() {
    this.x2u = new HashMap<>();
    this.u2x = new HashMap<>();
    this.wait = new HashMap<>();
    this.waitLock = new HashMap<>();
    this.waitU = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  // 不需要等待则返回null，否则返回锁对象
  // 会造成死锁则抛出异常
  public Lock add(long xid, long uid) throws Exception {
    lock.lock();
    try {
      if (isInList(x2u, xid, uid)) { // xid已经获得uid了，因此不需要等待
        return null;
      }
      if (!u2x.containsKey(uid)) { // 说明uid此时并没有被获取，因此可以设置之后，直接获取到，不需要等待
        u2x.put(uid, xid);
        putInfoList(x2u, xid, uid);
        return null;
      }
      waitU.put(xid, uid);  // 记录xid在等待uid
      putInfoList(wait, xid, uid); // 记录在等待uid的xid，感觉这里有点问题，xid和uid位置应该调换一下
      if (hasDeadLock()) { // 如果存在死锁，就撤销这条边，不允许添加，并撤销该事务
        waitU.remove(xid);
        removeFromList(wait, uid, xid);
        throw Error.DeadLockException;
      }
      // 这里是判断没有死锁后，并且uid已经被获取到了，因此需要等待
      // 设置xid以及对应lock，返回lock
      Lock l = new ReentrantLock();
      l.lock();  // 这里加锁返回后，要注意在selectNewXid方法里面，会打开这个锁，才能真正解锁
      waitLock.put(xid, l);
      return l;

    } finally {
      lock.unlock();
    }
  }

  // 当事务被commit,或者abort的时候，就会移除这个xid
  public void remove(long xid) {
    lock.lock();
    try {
      List<Long> l = x2u.get(xid);  // 获得这个xid持有的uid
      if (l != null) {
        while(l.size() > 0) {
          Long uid = l.remove(0);  // 从列表的头部开始遍历，进行分配
          selectNewXid(uid);
        }
      }
      waitU.remove(xid);  // 分配完之后，再移除waitU，说明它不再等待uid了
      x2u.remove(xid); // 移除xid
      waitLock.remove(xid); // 移除xid对应的lock
    } finally {
      lock.unlock();
    }
  }

  // 从等待队列中选择一个xid来占用
  private void selectNewXid(Long uid) {
    u2x.remove(uid);  // 首先解除uid和xid的绑定关系
    List<Long> l = wait.get(uid); // 获得等待uid的所有xid列表
    if (l == null) return;
    assert l.size() > 0;

    while (l.size() > 0) {
      long xid = l.remove(0); // 从xid列表的头部开始取
      if (!waitLock.containsKey(xid)) { // 则说明这个xid已经不再等待这个uid了，可能被commit/abort了
        continue;
      } else {
        u2x.put(uid, xid); // 此时uid被xid持有了
        Lock lo = waitLock.remove(xid); // 移除xid正在等待的锁
        waitU.remove(xid); // 移除xid等待uid的关系
        lo.unlock(); // 解锁
        break;
      }
    }
    if(l.size() == 0) wait.remove(uid); // 如果已经没有uid的xid了，直接移除
  }


  private Map<Long, Integer> xidStamp;
  private int stamp;

  // 检测死锁的方法就是从各个xid开始，进行图的深度遍历，如果遇到之前的标识，则说明死锁了
  private boolean hasDeadLock() {
    xidStamp = new HashMap<>();
    stamp = 1;
    for (long xid : x2u.keySet()) {
      Integer s = xidStamp.get(xid);
      if (s != null && s > 0) {
        continue;
      }
      stamp++;
      if (dfs(xid)) {
        return true;
      }
    }
    return false;
  }

  private boolean dfs(Long xid) {
    Integer stp = xidStamp.get(xid);
    if (stp != null && stp == stamp) { // 遇到之前的标识，则说明死锁了
      return true;
    }
    if (stp != null && stp < stamp) {
      return false;
    }
    xidStamp.put(xid, stamp);

    Long uid = waitU.get(xid); // 得到xid持有的资源uid
    if (uid == null) return false; // 如果uid为null，则必不可能形成死锁
    Long x = u2x.get(uid); // 得到持有uid的xid，并继续进行遍历
    assert x != null;
    return dfs(x);
  }


  public void removeFromList(Map<Long ,List<Long>> listMap, long uid0, long uid1) {
    List<Long> l = listMap.get(uid0);
    if (l == null) return;
    Iterator<Long> i = l.iterator();
    while (i.hasNext()) {
      long e = i.next();
      if (e == uid1) {
        i.remove();
        break;
      }
    }
    if (l.size() == 0) {
      listMap.remove(uid0);
    }
  }


  private void putInfoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
    if (!listMap.containsKey(uid0)) {
      listMap.put(uid0, new ArrayList<>());
    }
    listMap.get(uid0).add(0, uid1);
  }



  private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
    List<Long> l = listMap.get(uid0);
    if (l == null) return false;
    Iterator<Long> i = l.iterator();
    while(i.hasNext()) {
      long e = i.next();
      if (e == uid1) {
        return true;
      }
    }
    return false;
  }
}
