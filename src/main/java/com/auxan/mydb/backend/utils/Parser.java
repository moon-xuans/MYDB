package com.auxan.mydb.backend.utils;

import com.google.common.primitives.Bytes;
import java.nio.ByteBuffer;
import java.util.Arrays;

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

  public static int parseInt(byte[] raw) {
    ByteBuffer buffer = ByteBuffer.wrap(raw);
    return buffer.getInt();
  }

  public static byte[] int2Byte(int value) {
    return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
  }

  /**
   * 转成字符串，先根据前四个字节，判断出字符串的size，读出字符串之后，并计算出下一个data的offset
   * @param raw
   * @return
   */
  public static ParseStringRes parseString(byte[] raw) {
    int length = parseInt(Arrays.copyOf(raw, 4));
    String str = new String(Arrays.copyOfRange(raw, 4, 4 + length));
    return new ParseStringRes(str, length + 4);
  }

  public static byte[] string2Byte(String str) {
    byte[] l = int2Byte(str.length());
    return Bytes.concat(l, str.getBytes());
  }

  public static long str2Uid(String key) {
    long seed = 13331;
    long res = 0;
    for (byte b : key.getBytes()) {
      res = res * seed + (long)b;
    }
    return res;
  }
}
