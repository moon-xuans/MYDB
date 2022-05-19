package com.auxan.mydb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * 随机数的工具类
 * @author axuan
 */
public class RandomUtil {

  /**
   * 根据给定长度，生成一个字节数组
   * @param length
   * @return
   */
  public static byte[] randomBytes(int length) {
    Random r = new SecureRandom();
    byte[] buf = new byte[length];
    r.nextBytes(buf);
    return buf;
  }
}
