package com.axuan.mydb.common.util;

import com.axuan.mydb.backend.utils.Panic;
import com.axuan.mydb.common.Error;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author axuan
 * @date 2022/5/21
 **/
public class FileUtil {


  private static final int READ_SIZE = 1024;

  /**
   *
   * @param fileName 绝对路径+文件名
   */
  public static void printFile(String fileName) {
    File f = new File(fileName);
    if (!f.exists()) {
      Panic.panic(Error.FileNotExistsException);
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

    ByteBuffer buf = ByteBuffer.allocate(READ_SIZE);
    try {
      fc.position(0);
      fc.read(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    byte[] array = buf.array();
    System.out.println(new String(array));
  }
}
