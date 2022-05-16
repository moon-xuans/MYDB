package com.auxan.mydb.backend.utils;

import java.nio.ByteBuffer;

/**
 * 用来做类型转换的工具类
 * @author axuan
 */
public class Parser {

  public static long parseLong(byte[] buf) {
    ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
    return buffer.getLong();
  }

  public static byte[] long2Byte(long value) {
    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
  }
}
