package com.auxan.mydb.backend.tm;

import com.auxan.mydb.backend.tm.impl.TransactionManagerImpl;
import com.auxan.mydb.backend.utils.Panic;
import com.auxan.mydb.common.Error;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 事务管理接口
 * @author axuan
 */
public interface TransactionManager {
  /**开启事务*/
  long begin();

  /**提交事务*/
  void commit(long xid);

  /**丢弃事务(也可以说是回滚)*/
  void abort(long xid);

  boolean isActive(long xid);

  boolean isCommitted(long xid);

  boolean isAborted(long xid);

  /** 用于关闭文件通道和文件资源*/
  void close();



  public static TransactionManagerImpl create(String path) {
    File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
    try {
      if (!f.createNewFile()) {
        Panic.panic(Error.FileExistsException);
      }
    } catch (Exception e) {
      Panic.panic(e);
    }
    if (!f.canRead() || !f.canWrite()) {
      Panic.panic(Error.FileCannotRWException);
    }

    FileChannel fc = null;
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(f, "rw");
      fc = raf.getChannel();
    } catch (FileNotFoundException e) {
      Panic.panic(e);
    }

    // 写空XID文件头
    ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
    try {
      fc.position(0);
      fc.write(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }

    return new TransactionManagerImpl(raf, fc);
  }

}
