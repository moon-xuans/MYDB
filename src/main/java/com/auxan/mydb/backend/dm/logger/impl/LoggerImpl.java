package com.auxan.mydb.backend.dm.logger.impl;

import com.auxan.mydb.backend.dm.logger.Logger;
import com.auxan.mydb.backend.utils.Panic;
import com.auxan.mydb.backend.utils.Parser;
import com.auxan.mydb.common.Error;
import com.google.common.primitives.Bytes;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志的实现类
 * 负责日志读写
 *
 * 日志的文件标准格式为:
 * [XCheckSum][Log1][Log2]...[LogN][BadTail]
 * XCheckSum 为后续所有日志计算的CheckSum，int类型
 *
 * 每条正确日志的格式为：
 * [Size][CheckSum][Data]
 * Size 4字节int 标识Data的长度
 * CheckSum 4字节int
 *
 * @author axuan
 */
public class LoggerImpl implements Logger {

  private static final int SEED = 13331;


  private static final int OF_SIZE = 0;

  private static final int OF_CHECKSUM = OF_SIZE + 4; // 校验值的长度

  private static final int OF_DATA = OF_CHECKSUM + 4; // 校验值+size的长度


  private RandomAccessFile file;

  private FileChannel fc;

  private Lock lock;


  private long position; // 当前日志指针的位置

  private long fileSize; // 初始化时记录，log操作不更新

  private int xCheckSum; // 用来记录总日志文件的CheckSum

  public LoggerImpl(RandomAccessFile file, FileChannel fc) {
    this.file = file;
    this.fc = fc;
    lock = new ReentrantLock();
  }

  public LoggerImpl(RandomAccessFile file, FileChannel fc, int xCheckSum) {
    this.file = file;
    this.fc = fc;
    this.xCheckSum = xCheckSum;
    lock = new ReentrantLock();
  }

  void init() {
    long size = 0;
    try {
      size = file.length();
    } catch (IOException e) {
      Panic.panic(e);
    }
    if (size < 4) {
      Panic.panic(Error.BadXidFileException);
    }

    ByteBuffer raw = ByteBuffer.allocate(4);
    try {
      fc.position(0);
      fc.read(raw);
    } catch (IOException e) {
      Panic.panic(e);
    }
    int xCheckSum = Parser.parseInt(raw.array());
    this.fileSize = size;
    this.xCheckSum = xCheckSum;

    checkAndRemoveTail();
  }


  // 可能写日志过程中，突然宕机，写入了一半日志，因此就是一个坏的日志文件，因此需要去除这个文件，保证数据的一致性
  // 检查并移除bad tail
  private void checkAndRemoveTail() {
    rewind();

    int xCheck = 0;
    while (true) {
      byte[] log = internNext();
      if (log == null) break;
      xCheck = calCheckSum(xCheck, log);
    }
    if (xCheck != xCheckSum) {
      Panic.panic(Error.BadXidFileException);
    }

    try {
      truncate(position); // 截断后面的坏的日志
    } catch (Exception e) {
      Panic.panic(e);
    }
    try {
      file.seek(position); // 这里我猜测先回到position，再rewind。
    } catch (IOException e) {
      Panic.panic(e);
    }
    rewind();
  }


  private int calCheckSum(int xCheck, byte[] log) {
    for (byte b : log) {
      xCheck = xCheckSum * SEED + b;
    }
    return xCheck;
  }

  private byte[] internNext() {
    // 这个position是应该从第一条日志，也就是position=4的时候开始计算,在rewind()方法中验证了这一点
    if (position + OF_DATA >= fileSize) {
      return null;
    }
    // 读取size
    ByteBuffer tmp = ByteBuffer.allocate(4);
    try {
      fc.position(position);
      fc.read(tmp);
    } catch (IOException e) {
      Panic.panic(e);
    }
    int size = Parser.parseInt(tmp.array());
    if (position + size + OF_DATA > fileSize) {
      return null;
    }
    ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
    try {
      fc.position(position);
      fc.read(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }

    byte[] log = buf.array();
    int checkSum1 = calCheckSum(0, Arrays.copyOfRange(log, OF_DATA, log.length)); // 根据data计算出校验值
    int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA)); // 再取出日志中的校验值
    if (checkSum1 != checkSum2) {
      return null;
    }
    position += log.length;
    return log;
  }


  @Override
  public void log(byte[] data) {
    byte[] log = wrap(data);
    ByteBuffer buf = ByteBuffer.wrap(log);
    lock.lock();
    try {
      fc.position(fc.size());
      fc.write(buf);
    } catch (IOException e) {
      Panic.panic(e);
    } finally {
      lock.unlock();
    }
    updateXCheckSum(log);
  }

  /**
   * 更新总的日志文件的checkSum
   * @param log
   */
  private void updateXCheckSum(byte[] log) {
    this.xCheckSum = calCheckSum(this.xCheckSum, log);
    try {
      fc.position(0);
      fc.write(ByteBuffer.wrap(Parser.int2Byte(xCheckSum)));
    } catch (IOException e) {
      Panic.panic(e);
    }
  }

  private byte[] wrap(byte[] data) {
    byte[] checkSum = Parser.int2Byte(calCheckSum(0, data));
    byte[] size = Parser.int2Byte(data.length);
    return Bytes.concat(size, checkSum, data);
  }

  @Override
  public void truncate(long x) throws Exception {
    lock.lock();
    try {
      fc.truncate(x);
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获得下一个日志的数据
   * @return
   */
  @Override
  public byte[] next() {
    lock.lock();
    try {
      byte[] log = internNext();
      if (log == null) return null;
      return Arrays.copyOfRange(log, OF_DATA, log.length);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void rewind() {
    position = 4;
  }

  @Override
  public void close() {
    try {
      fc.close();
      file.close();
    } catch (IOException e) {
      Panic.panic(e);
    }
  }
}
