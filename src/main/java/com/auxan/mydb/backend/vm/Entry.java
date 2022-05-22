package com.auxan.mydb.backend.vm;

import com.auxan.mydb.backend.dm.dataItem.DataItem;
import com.auxan.mydb.backend.utils.Parser;
import com.auxan.mydb.backend.vm.impl.VersionManagerImpl;
import com.auxan.mydb.common.SubArray;
import com.google.common.primitives.Bytes;
import java.util.Arrays;

/**
 * VM向上层抽象出entry
 * [XMIN][XMAX][data]
 * @author axuan
 * @date 2022/5/21
 **/
public class Entry {

  private static final int OF_XMIN = 0;
  private static final int OF_XMAX = OF_XMIN + 8;
  private static final int OF_DATA = OF_XMAX + 8;

  private long uid;
  private DataItem dataItem;
  private VersionManager vm;

  public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
    Entry entry = new Entry();
    entry.uid = uid;
    entry.dataItem = dataItem;
    entry.vm = vm;
    return entry;
  }

  public static Entry loadEntry(VersionManager vm, long uid) throws Exception{
    DataItem di = ((VersionManagerImpl) vm).dm.read(uid);
    return newEntry(vm, di, uid);
  }





  public static byte[] wrapEntryRaw(long xid, byte[] data) {
    byte[] xmin = Parser.long2Byte(xid);
    byte[] xmax = new byte[8];
    return Bytes.concat(xmin, xmax, data);
  }


  public void release() {
    ((VersionManagerImpl)vm).releaseEntry(this);
  }

  public void remove() {
    dataItem.release();
  }


  // 以拷贝的形式返回内容
  public byte[] data() {
    dataItem.rLock();
    try {
      SubArray sa = dataItem.data();
      // 这里要减去XMIN,XMAX的长度，但是不知道和那边减去OF_DATA有什么区别
      // 我的理解是XMIN，XMAX也属于那边Data的一部分，不过这里只要有效数据
      byte[] data = new byte[sa.end - sa.start - OF_DATA];
      System.arraycopy(sa.raw, sa.start + OF_DATA, data, 0, data.length);
      return data;
    } finally {
      dataItem.rUnLock();
    }
  }


  public long getXmin() {
    dataItem.rLock();
    try {
      SubArray sa = dataItem.data();
      return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMIN, sa.start + OF_XMAX));
    } finally {
      dataItem.rUnLock();
    }
  }


  public long getXmax() {
    dataItem.rLock();
    try {
      SubArray sa = dataItem.data();
      return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMAX, sa.start + OF_DATA));
    } finally {
      dataItem.rUnLock();
    }
  }

  public void setMax(long xid) {
    dataItem.before();
    try {
      SubArray sa = dataItem.data();
      System.arraycopy(Parser.long2Byte(xid),0, sa.raw, sa.start + OF_XMAX, 8);
    } finally {
      dataItem.after(xid);
    }
  }

  public long getUid() {
    return uid;
  }
}
