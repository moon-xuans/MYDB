package com.auxan.mydb.backend.utils;

import java.nio.ByteBuffer;

/**
 * 用来做类型转换的工具类
 * @author axuan
 */
public class Parser {

  public static byte[] short2Byte(short value) {
    return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
  }

  public static long parseLong(byte[] buf) {
    ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
    return buffer.getLong();
  }

  public static byte[] long2Byte(long value) {
    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
  }

  public static short parseShort(byte[] raw) {
    ByteBuffer buffer = ByteBuffer.wrap(raw, 0, 2);
    return buffer.getShort();
  }
}
