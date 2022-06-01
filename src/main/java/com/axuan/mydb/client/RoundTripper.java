package com.axuan.mydb.client;

import com.axuan.mydb.transport.Package;
import com.axuan.mydb.transport.Packager;
import java.io.IOException;

/**
 * @description 这里是做了封装，一次发送+一次接收
 * @author axuan
 * @date 2022/5/29
 **/
public class RoundTripper {

  private Packager packager;

  public RoundTripper(Packager packager) {
    this.packager = packager;
  }

  public Package roundTrip(Package pkg) throws Exception {
    packager.send(pkg);
    return packager.receive();
  }

  public void close() throws IOException {
    packager.close();
  }

}
