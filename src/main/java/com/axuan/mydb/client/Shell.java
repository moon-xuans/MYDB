package com.axuan.mydb.client;

import java.util.Scanner;

/**
 * @author axuan
 * @date 2022/5/29
 **/
public class Shell {

  private Client client;

  public Shell(Client client) {
    this.client = client;
  }

  public void run() {
    Scanner sc = new Scanner(System.in);
    try {
      while(true) {
        System.out.println(":> ");
        String statStr = sc.nextLine();
        if ("exit".equals(statStr) || "quit".equals(statStr)) {
          break;
        }
        try {
          byte[] res = client.execute(statStr.getBytes());
          System.out.println(new String(res));
        } catch (Exception e) {
          System.out.println(e.getMessage());
        }
      }
    } finally {
      sc.close();
      client.close();
    }
  }
}
