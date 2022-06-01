package com.axuan.mydb.backend.im;

import com.axuan.mydb.backend.dm.DataManager;
import com.axuan.mydb.backend.dm.dataItem.DataItem;
import com.axuan.mydb.backend.im.Node.InsertAndSplitRes;
import com.axuan.mydb.backend.im.Node.LeafSearchRangeRes;
import com.axuan.mydb.backend.im.Node.SearchNextRes;
import com.axuan.mydb.backend.tm.impl.TransactionManagerImpl;
import com.axuan.mydb.backend.utils.Parser;
import com.axuan.mydb.backend.common.SubArray;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 由于B+树在插入删除时，会动态调整，根节点不是固定节点，于是设置一个bootDataItem，该DataItem中存储
 * 了根节点的UID。可以注意到，IM在操作DM时，使用的事务都是SUPER_XID
 * @author axuan
 * @date 2022/5/22
 **/
public class BPlusTree {
  DataManager dm;
  long bootUid;
  DataItem bootDataItem;
  Lock bootLock;


  public static long create(DataManager dm) throws Exception {
    byte[] rawRoot = Node.newNilRootRaw();
    long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
    return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
  }

  public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
    DataItem bootDataItem = dm.read(bootUid);
    assert bootDataItem != null;
    BPlusTree t = new BPlusTree();
    t.bootUid = bootUid;
    t.dm = dm;
    t.bootDataItem = bootDataItem;
    t.bootLock = new ReentrantLock();
    return t;
  }

  private long rootUid() {
    bootLock.lock();
    try {
      SubArray sa = bootDataItem.data();
      return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8)); // 返回data中的uid
    } finally {
      bootLock.unlock();
    }
  }


  private void updateRootUid(long left, long right, long rightKey) throws Exception {
    bootLock.lock();
    try {
      byte[] rootRaw = Node.newRootRaw(left, right, rightKey);  // 创建了一个新的根节点
      long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw); // 插入到dm中
      bootDataItem.before();
      SubArray diRaw = bootDataItem.data();
      System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8); // 将根节点的uid记录在diRaw中，相当于更新其rootUid
      bootDataItem.after(TransactionManagerImpl.SUPER_XID);
    } finally {
      bootLock.unlock();
    }
  }


  private long searchLeaf(long nodeUid, long key) throws Exception {
    Node node = Node.loadNode(this, nodeUid);  // 先构建出来这个节点
    boolean isLeaf = node.isLeaf(); // 判断是否是叶子节点
    node.release(); // 释放内存

    if (isLeaf) { // 如果是
      return nodeUid; // 直接返回
    } else {
      long next = searchNext(nodeUid, key); // 如果不是，寻找下一个节点
      return searchLeaf(next, key); // 递归寻找
    }
  }

  private long searchNext(long nodeUid, long key) throws Exception {
    while(true) {
      Node node = Node.loadNode(this, nodeUid);  // 首先通过B+树，uid构建出node
      SearchNextRes res = node.searchNext(key); // 根据key进行查找
      node.release(); // 先释放节点，防止撑爆内存
      if (res.uid != 0) return res.uid; // 如果找到不为0，则返回，就是我们要找的下一个节点
      nodeUid = res.siblingUid; // 如果没找到，从它的兄弟节点开始找
    }
  }

  public List<Long> search(int key) throws Exception {
    return searchRange(key, key);
  }

  public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
    long rootUid = rootUid();
    long leafUid = searchLeaf(rootUid, leftKey);
    List<Long> uids = new ArrayList<>();
    while(true) {
      Node leaf = Node.loadNode(this, leafUid);
      LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
      leaf.release();
      uids.addAll(res.uids);
      if (res.siblingUid == 0) {
        break;
      } else {
        leafUid = res.siblingUid;
      }
    }
    return uids;
  }

  public void insert(long key, long uid) throws Exception {
    long rootUid = rootUid();
    InsertRes res = insert(rootUid, uid, key);
    assert res != null;
    if (res.newNode != 0) {
      updateRootUid(rootUid, res.newNode, res.newKey);
    }
  }



  class InsertRes {
    long newNode, newKey;
  }

  private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
    Node node = Node.loadNode(this, nodeUid);
    boolean isLeaf = node.isLeaf();
    node.release();

    InsertRes res = null;
    if (isLeaf) {
      res = insertAndSplit(nodeUid, uid, key);
    } else {
      long next = searchNext(nodeUid, key);
      InsertRes ir = insert(next, uid, key);
      if (ir.newNode != 0) {
        res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
      } else {
        res = new InsertRes();
      }
    }
    return res;
  }

  private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
    while(true) {
      Node node = Node.loadNode(this, nodeUid);
      InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
      node.release();
      if (iasr.siblingUid != 0) {
        nodeUid = iasr.siblingUid;
      } else {
        InsertRes res = new InsertRes();
        res.newNode = iasr.newSon;
        res.newKey = iasr.newKey;
        return res;
      }
    }
  }

  public void close() {
    bootDataItem.release();
  }
}
