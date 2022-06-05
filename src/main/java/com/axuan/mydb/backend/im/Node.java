package com.axuan.mydb.backend.im;

import com.axuan.mydb.backend.dm.dataItem.DataItem;
import com.axuan.mydb.backend.tm.impl.TransactionManagerImpl;
import com.axuan.mydb.backend.utils.Parser;
import com.axuan.mydb.backend.common.SubArray;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * @author axuan
 * @date 2022/5/22
 **/
public class Node {
  static final int IS_LEAF_OFFSET = 0; // leaf标志位占1位
  static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;  // 节点个数占2位
  static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;  // 兄弟uid占8位
  static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;// 整个节点头部占1 + 2 + 8 = 11

  static final int BALANCE_NUMBER = 32;
  static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2); // 节点大小 = 节点头部 + 一个子节点的大小 * ？

  BPlusTree tree;
  DataItem dataItem;
  SubArray raw;
  long uid;

  /**
   * 设置该节点是否是叶子节点
   * @param raw
   * @param isLeaf
   */
  static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
    if (isLeaf) {
      raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;  // 对应位置是1，则是叶子节点
    } else {
      raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;  // 对应位置是0，则是非叶子节点
    }
  }

  static boolean getRawIfLeaf(SubArray raw) {
    return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
  }


  /**
   * 设置该节点下的节点数量
   * @param raw
   * @param noKeys
   */
  static void setRawNoKeys(SubArray raw, int noKeys) {
    System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
  }

  static int getRawNoKeys(SubArray raw) {
    return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
  }

  /**
   * 设置该节点的兄弟的uid
   * @param raw
   * @param sibling
   */
  static void setRawSibling(SubArray raw, long sibling) {
    System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
  }

  static long getRawSibling(SubArray raw) {
    return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
  }


  /**
   * 设置该节点的第n个孩子的uid
   * @param raw
   * @param uid
   * @param kth
   */
  static void setRawKthSon(SubArray raw, long uid, int kth) {
    int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);  // 这里的8 * 2，其中一个8是uid，另外一个8是key
    System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
  }

  /**
   * 返回该节点的第n个孩子的uid
   * @param raw
   * @param kth
   * @return
   */
  static long getRawKthSon(SubArray raw, int kth) {
    int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
    return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
  }


  /**
   * 设置该节点的第n个节点的key
   * @param raw
   * @param key
   * @param kth
   */
  static void setRawKthKey(SubArray raw, long key, int kth) {
    int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8; // 8 * 2 * kth先找到对应的节点位置，然后+8找到对应key位置
    System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
  }

  /**
   * 获取该节点的第n个节点的key
   * @param raw
   * @param kth
   * @return
   */
  static long getRawKthKey(SubArray raw, int kth) {
    int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
    return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
  }


  static void copyRawFromKth(SubArray from, SubArray to, int kth) {
    int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
    System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
  }



  /**
   * 初始化一个root节点
   * @param left 左节点的uid
   * @param right 右节点的uid
   * @param key 初始键值，不是很理解，我理解是索引上的值
   * @return
   */
  static byte[] newRootRaw(long left, long right, long key) {
    SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

    setRawIsLeaf(raw, false);
    setRawNoKeys(raw, 2);
    setRawSibling(raw, 0);
    setRawKthSon(raw, left, 0);
    setRawKthKey(raw, key, 0);
    setRawKthSon(raw, right, 1);
    setRawKthKey(raw, Long.MAX_VALUE, 1);

    return raw.raw;
  }

  /**
   * 初始一个空的根节点
   * @return
   */
  static byte[] newNilRootRaw() {
    SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

    setRawIsLeaf(raw, true);
    setRawNoKeys(raw, 0);
    setRawSibling(raw, 0);

    return raw.raw;
  }



  static Node loadNode(BPlusTree bTree, long uid) throws Exception {
    DataItem di = bTree.dm.read(uid);
    assert di != null;
    Node n = new Node();
    n.tree = bTree;
    n.dataItem = di;
    n.raw = di.data();
    n.uid = uid;
    return n;
  }

  public void release() {
    dataItem.release();
  }

  public boolean isLeaf() {
    dataItem.rLock();
    try {
      return getRawIfLeaf(raw);
    } finally {
      dataItem.rUnLock();
    }
  }


  class SearchNextRes {
    long uid;
    long siblingUid;
  }

  /**
   * 用于插入操作
   * 寻找对应key的UID，如果找不到，就返回兄弟节点的UID
   * @param key
   * @return
   */
  public SearchNextRes searchNext(long key) {
    dataItem.rLock();
    try {
      SearchNextRes res = new SearchNextRes();
      int noKeys = getRawNoKeys(raw);
      for (int i = 0; i < noKeys; i++) {
        long ik = getRawKthKey(raw, i);
        if (key < ik) {   // 这里我的理解是，只要找到对应的小于key的节点即可，就可以顺着索引寻找下去
          res.uid  = getRawKthSon(raw, i);
          res.siblingUid = 0;
          return res;
        }
      }
      res.uid = 0;
      res.siblingUid = getRawSibling(raw); // 若没有找到，则返回其兄弟节点
      return res;
    } finally {
      dataItem.rUnLock();
    }
  }

  class LeafSearchRangeRes {
    List<Long> uids;
    long siblingUid;
  }

  /**
   * 用于寻找操作
   * 在当前节点进行返回进行范围查找，范围是[leftKey, rightkey]，这里约定如果rightKey大于等于
   * 该节点的最大的key，则还同时返回兄弟节点的UID，方便继续搜索下一个节点
   * @param leftKey
   * @param rightKey
   * @return
   */
  public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
    dataItem.rLock();

    try {
      int noKeys = getRawNoKeys(raw);
      int kth = 0;
      while (kth < noKeys) {
        long ik = getRawKthKey(raw, kth);
        if (ik >= leftKey) {
          break;
        }
        kth++;
      }
      ArrayList<Long> uids = new ArrayList<>();
      while (kth < noKeys) {
        long ik = getRawKthKey(raw, kth);
        if (ik <= rightKey) {
          uids.add(getRawKthSon(raw, kth));
          kth++;
        } else {
          break;
        }
      }
      long siblingUid = 0;
      if (kth == noKeys) { // 如果说寻找到头了，那末就要返回其兄弟节点，方便继续搜索下一个节点
        siblingUid = getRawSibling(raw);
      }
      LeafSearchRangeRes res = new LeafSearchRangeRes();
      res.uids = uids;
      res.siblingUid = siblingUid;
      return res;
    } finally {
      dataItem.rUnLock();
    }
  }

  class InsertAndSplitRes {
    long siblingUid, newSon, newKey;
  }


  public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
    boolean success = false;
    Exception err = null;
    InsertAndSplitRes res = new InsertAndSplitRes();

    dataItem.before();
    try {
      success = insert(uid, key);
      if (!success) {
        res.siblingUid = getRawSibling(raw);
        return res;
      }
      if (needSplit()) {
        try {
          SplitRes r = split();
          res.newSon = r.newSon;
          res.newKey = r.newKey;
          return res;
        } catch (Exception e) {
          err = e;
          throw e;
        }
      } else {
        return res;
      }
    } finally {
      if (err == null && success) {
        dataItem.after(TransactionManagerImpl.SUPER_XID);
      } else {
        dataItem.unBefore();
      }
    }
  }


  // 当一行的节点数量达到64个之后i，需要分裂
  private boolean needSplit() {
    return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
  }

  class SplitRes {
    long newSon, newKey;
  }

  private SplitRes split() throws Exception {
    SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
    setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
    setRawNoKeys(nodeRaw, BALANCE_NUMBER);
    setRawSibling(nodeRaw, getRawSibling(raw));
    copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER); // 将原来的节点全部移到nodeRaw
    long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw); // 然后将这些节点持久化，并得到其对应的uid
    setRawNoKeys(raw, BALANCE_NUMBER); // 设置这一层的节点数量
    setRawSibling(raw, son); // 则刚才的那个节点就是儿子节点

    SplitRes res = new SplitRes();
    res.newSon = son;
    res.newKey = getRawKthKey(nodeRaw, 0); // 该儿子的节点的最后一个key
    return res;
  }


  private boolean insert(long uid, long key) {
    int noKeys = getRawNoKeys(raw);
    int kth = 0;
    while (kth < noKeys) {
      long ik = getRawKthKey(raw, kth);
      if (ik < key) {
        kth++;
      } else {
        break;
      }
    }
    if (kth == noKeys && getRawSibling(raw) != 0) return false;

    if (getRawIfLeaf(raw)) {
      shiftRawKth(raw, kth);
      setRawKthKey(raw, key, kth); // 设置到之前较大的节点上，key是主键，uid是资源位置
      setRawKthSon(raw, uid, kth);
      setRawNoKeys(raw, noKeys + 1);
    } else {
      long kk = getRawKthKey(raw, kth);
      setRawKthKey(raw, key, kth);
      shiftRawKth(raw, kth + 1);
      setRawKthKey(raw, kk, kth + 1);
      setRawKthSon(raw, uid, kth + 1);
      setRawNoKeys(raw, noKeys + 1);
    }
    return true;
  }

  /**
   * 将节点整体上右移，给较小的节点移出位置
   * @param raw
   * @param kth
   */
  private void shiftRawKth(SubArray raw, int kth) {
    int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
    int end = raw.start + NODE_SIZE - 1;
    for (int i = end; i >= begin; i--) {
      raw.raw[i] = raw.raw[i - (8 * 2)];
    }
  }

}
