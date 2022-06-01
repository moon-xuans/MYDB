package com.axuan.mydb.transport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * 这里通过Transporter来进行数据流的写入写出，为了保证特殊字符，因此传输的时候转成十六进制。
 * @author axuan
 * @date 2022/5/29
 **/
public class Transporter {
  private Socket socket;
  private BufferedReader reader;
  private BufferedWriter writer;

  public Transporter(Socket socket) throws IOException {
    this.socket = socket;
    this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
  }

  public void send(byte[] data) throws IOException {
    String raw = hexEncode(data);
    writer.write(raw);
    writer.flush();
  }

  public byte[] receive() throws Exception {
    String line = reader.readLine();
    if (line == null) {
      close();
    }
    return hexDecode(line);
  }

  public void close() throws IOException {
    writer.close();
    reader.close();
    socket.close();
  }



  private String hexEncode(byte[] buf) {
    return Hex.encodeHexString(buf, true) + "\n";
  }

  private byte[] hexDecode(String buf) throws DecoderException {
    return Hex.decodeHex(buf);
  }

}
