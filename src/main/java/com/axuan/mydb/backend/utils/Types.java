package com.axuan.mydb.backend.utils;

/**
 * 用于类型转换的工具类
 * @author axuan
 */
public class Types {

  public static long addressToUid(int pgNo, short offset) {
    long u0 = (long)pgNo;
    long u1 = (long)offset;
    return u0 << 32 | u1;
  }
}
