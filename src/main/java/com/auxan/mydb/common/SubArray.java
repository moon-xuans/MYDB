package com.auxan.mydb.common;

/**
 * 由于java语言的特性，就算使用subArray也无法使用同一块内存，因此使用这种方式，达到内存共享
 * @author axuan
 */
public class SubArray {
  public byte[] raw;
  public int start;
  public int end;

  public SubArray(byte[] raw, int start, int end) {
    this.raw = raw;
    this.start = start;
    this.end = end;
  }
}
