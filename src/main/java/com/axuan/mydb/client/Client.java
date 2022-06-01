package com.axuan.mydb.client;

import com.axuan.mydb.transport.Package;
import com.axuan.mydb.transport.Packager;

/**
 * @author axuan
 * @date 2022/5/29
 **/
public class Client {
  private RoundTripper rt;

  public Client(Packager packager) {
    this.rt = new RoundTripper(packager);
  }

  /**
   * 客户端给定sql，进行执行
   * @param stat
   * @return
   * @throws Exception
   */
  public byte[] execute(byte[] stat) throws Exception {
    Package pkg = new Package(stat, null);
    Package resPkg = rt.roundTrip(pkg); // 执行后接收包
    if (resPkg.getErr() != null) { // 如果包中异常不为空，抛出异常
      throw resPkg.getErr();
    }
    return resPkg.getData(); // 没有异常的话，就返回数据
  }

  public void close() {
    try {
      rt.close();
    } catch (Exception e) {
    }
  }
}
