package com.auxan.mydb.backend.utils;

/**
 * 出现底层代码异常，用来终止程序
 * @author axuan
 */
public class Panic {

  public static void panic(Exception err) {
    err.printStackTrace();
    System.exit(1);
  }
}
