package com.axuan.mydb.backend.tm.impl;

import com.axuan.mydb.backend.tm.TransactionManager;
import com.axuan.mydb.backend.utils.Panic;
import com.axuan.mydb.backend.utils.Parser;
import com.axuan.mydb.common.Error;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务管理的实现类
 * @author axuan
 */
public class TransactionManagerImpl implements TransactionManager {

  // XID文件头长度
  public static final int LEN_XID_HEADER_LENGTH = 8;

  // 每个事务的占用长度
  private static final int XID_FIELD_SIZE = 1;

  // 事务的三种状态
  private static final byte FIELD_TRAN_ACTIVE = 0;
  private static final byte FIELD_TRAN_COMMITTED = 1;
  private static final byte FIELD_TRAN_ABORTED = 2;

  // 超级事务,永远为committed状态
  public static final long SUPER_XID = 0;

  public static final String XID_SUFFIX = ".xid";

  private RandomAccessFile file;
  private FileChannel fc;
  private long xidCounter;
  private Lock counterLock;

  public TransactionManagerImpl(RandomAccessFile file, FileChannel fc) {
    this.file = file;
    this.fc = fc;
    this.counterLock = new ReentrantLock();
    checkXIDCounter();
  }

  /**
   * 检查XID文件是否合法
   * 读取XID_FILE_HEADER中的xidCounter,根据它计算文件的理论长度，对比实际长度
   */
  private void checkXIDCounter() {
    long fileLen = 0;
    try {
      fileLen = file.length();
    } catch (IOException e) {
      Panic.panic(Error.BadXidFileException);
    }
    if (fileLen < LEN_XID_HEADER_LENGTH) {
      Panic.panic(Error.BadXidFileException);
    }

    ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
    try {
      fc.position(0);
      fc.read(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    this.xidCounter = Parser.parseLong(buf.array());
    long end = getXidPosition(this.xidCounter + 1);
    if (end != fileLen) {
      Panic.panic(Error.BadXidFileException);
    }
  }

  // 根据事务xid取得其在xid文件对应的位置
  private long getXidPosition(long xid) {
    return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
  }

  // 更新xid事务的状态为status
  private void updateXID(long xid, byte status) {
    long offset = getXidPosition(xid);
    byte[] tmp = new byte[XID_FIELD_SIZE];
    tmp[0] = status;
    ByteBuffer buf = ByteBuffer.wrap(tmp);
    try {
      fc.position(offset);
      fc.write(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    try {
      fc.force(false);
    } catch (IOException e) {
      Panic.panic(e);
    }
  }


  // 将XID加一，并更新XID Header
  private void incrXIDCounter() {
    xidCounter++;
    ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
    try {
      fc.position(0);
      fc.write(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    try {
      fc.force(false);
    } catch (IOException e) {
      Panic.panic(e);
    }
  }

  @Override
  public long begin() {
    counterLock.lock();
    try {
      long xid = xidCounter + 1;
      updateXID(xid, FIELD_TRAN_ACTIVE);
      incrXIDCounter();
      return xid;
    } finally {
     counterLock.unlock();
    }
  }

  @Override
  public void commit(long xid) {
    updateXID(xid, FIELD_TRAN_COMMITTED);
  }

  @Override
  public void abort(long xid) {
    updateXID(xid, FIELD_TRAN_ABORTED);
  }

  // 检测XID事务是否处于status状态
  private boolean checkXID(long xid, byte status) {
    long offset = getXidPosition(xid);
    ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
    try {
      fc.position(offset);
      fc.read(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    return buf.array()[0] == status;
  }

  @Override
  public boolean isActive(long xid) {
    if (xid == SUPER_XID) return false;
    return checkXID(xid, FIELD_TRAN_ACTIVE);
  }

  @Override
  public boolean isCommitted(long xid) {
    if (xid == SUPER_XID) return true;
    return checkXID(xid, FIELD_TRAN_COMMITTED);
  }

  @Override
  public boolean isAborted(long xid) {
    if (xid == SUPER_XID) return false;
    return checkXID(xid, FIELD_TRAN_ABORTED);
  }

  @Override
  public void close() {
    try {
      fc.close();
      file.close();
    } catch (Exception e) {
      Panic.panic(e);
    }
  }


}
