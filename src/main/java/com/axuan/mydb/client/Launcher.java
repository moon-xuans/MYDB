package com.axuan.mydb.client;

import com.axuan.mydb.transport.Encoder;
import com.axuan.mydb.transport.Packager;
import com.axuan.mydb.transport.Transporter;
import java.io.IOException;
import java.net.Socket;

/**
 * @author axuan
 * @date 2022/5/29
 **/
public class Launcher {

  public static void main(String[] args) throws IOException {
    Socket socket = new Socket("127.0.0.1", 9999);
    Encoder e = new Encoder();
    Transporter t = new Transporter(socket);
    Packager packager = new Packager(t, e);

    Client client = new Client(packager);
    Shell shell = new Shell(client);
    shell.run();
  }
}
