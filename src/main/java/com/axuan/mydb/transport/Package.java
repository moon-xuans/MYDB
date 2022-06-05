package com.axuan.mydb.transport;

/**
 * 在数据传输的时候都是通过包来传递的
 * [flag][data]
 * @author axuan
 * @date 2022/5/29
 **/
public class Package {
  byte[] data;
  Exception err;

  public Package(byte[] data, Exception err) {
    this.data = data;
    this.err = err;
  }

  public byte[] getData() {
    return data;
  }

  public Exception getErr() {
    return err;
  }
}
