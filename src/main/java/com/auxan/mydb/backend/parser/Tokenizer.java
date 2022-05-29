package com.auxan.mydb.backend.parser;

import com.auxan.mydb.common.Error;

/**
 * @author axuan
 * @date 2022/5/24
 **/
public class Tokenizer {
  private byte[] stat;
  private int pos;
  private String currentToken;
  private boolean flushToken;
  private Exception err;

  public Tokenizer(byte[] stat) {
    this.stat = stat;
    this.pos = 0;
    this.currentToken = "";
    this.flushToken = true;
  }

  /**
   * 返回字符串
   * @return
   * @throws Exception
   */
  public String peek() throws Exception {
    if (err != null) {
      throw err;
    }
    if (flushToken) { // 如果为ture，则说明需要刷新，获取下一个字符串
      String token = null;
      try {
        token = next();
      } catch (Exception e) {
        err = e;
        throw e;
      }
      currentToken = token;
      flushToken = false;
    }
    return currentToken;
  }

  /**
   * 放入下一个
   */
  public void pop() {
    flushToken = true;
  }


  public byte[] errStat() {
    byte[] res = new byte[stat.length + 3];
    System.arraycopy(stat, 0, res, 0, pos);
    System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
    System.arraycopy(stat, pos, res, pos + 3, stat.length - pos);
    return res; // 输出错误[(...~pos)<< (pos~...)]
  }

  private void popByte() {
    pos++;
    if (pos > stat.length) {
      pos = stat.length;
    }
  }

  private Byte peekByte() {
    if (pos == stat.length) {
      return null;
    }
    return stat[pos];
  }

  // 返回下个字符串
  private String next() throws Exception {
    if (err != null) {
      throw err;
    }
    return nextMetaState();
  }

  private String nextMetaState() throws Exception {
    while (true) { // 这里主要是要找到不为空的那个字符
      Byte b = peekByte();
      if (b == null) {
        return "";
      }
      if (!isBlank(b)) {
        break;
      }
      popByte();
    }
    byte b = peekByte();
    if (isSymbol(b)) { // 如果是判断符号，直接返回
      popByte();
      return new String(new byte[]{b});
    } else if (b == '"' || b == '\'') {
      return nextQuoteState();
    } else if (isAlphaBeta(b) || isDigit(b)) {
      return nextTokenState();
    } else {
      err = Error.InvalidCommandException;
      throw err;
    }
  }

  private String nextTokenState() {
    StringBuffer sb = new StringBuffer();
    while(true) {
      Byte b = peekByte();
      if (b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) { // 如果这个字符不是符号，数字等
        if (b != null && isBlank(b)) { // 这个字符是空
          popByte();
        }
        return sb.toString(); // 直接返回
      }
      sb.append(new String(new byte[]{b}));
      popByte();
    }
  }

  static boolean isDigit(byte b) {
    return (b >= '0' && b <= '9');
  }

  static boolean isAlphaBeta(byte b) {
    return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
  }

  /**
   * 返回转义\后面的字符串
   * @return
   * @throws Exception
   */
  private String nextQuoteState() throws Exception {
    Byte quote = peekByte();
    popByte();
    StringBuffer sb = new StringBuffer();
    while (true) {
      Byte b = peekByte();
      if (b == null) {
        err = Error.InvalidCommandException;
        throw err;
      }
      if (b == quote) {
        popByte();
        break;
      }
      sb.append(new String(new byte[]{b}));
      popByte();
    }
    return sb.toString();
  }


  static boolean isSymbol(byte b) {
    return (b == '>' || b == '<' || b == '=' || b == '*' ||
        b == ',' || b == '(' || b == ')');
  }

  static boolean isBlank(byte b) {
    return (b == '\n' || b == ' ' || b == '\t');
  }
}
