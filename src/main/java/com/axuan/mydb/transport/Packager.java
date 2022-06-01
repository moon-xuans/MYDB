package com.axuan.mydb.transport;

import java.io.IOException;

/**
 * @descrption Packager是Encoder和Transporter的结合体，对外提供封装好的send和receive方法
 * @author axuan
 * @date 2022/5/29
 **/
public class Packager {

  private Transporter transporter;
  private Encoder encoder;

  public Packager(Transporter transporter, Encoder encoder) {
    this.transporter = transporter;
    this.encoder = encoder;
  }

  public void send(Package pkg) throws IOException {
    byte[] data = encoder.encode(pkg);
    transporter.send(data);
  }

  public Package receive() throws Exception {
    byte[] data = transporter.receive();
    return encoder.decode(data);
  }

  public void close() throws IOException {
    transporter.close();
  }
}
