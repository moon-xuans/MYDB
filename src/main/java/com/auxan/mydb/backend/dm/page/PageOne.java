package com.auxan.mydb.backend.dm.page;

import com.auxan.mydb.backend.utils.RandomUtil;
import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100～107字节处填入一个随机字节，db关闭时将其拷贝到108～115字节
 * 用于判断上一次数据库是否正常关闭
 * @author axuan
 */
public class PageOne {

  private static final int OF_VC = 100;  // 校验值的起始offset

  private static final int LEN_VC = 8; // 校验值的长度



  public static void setVcOpen(Page pg) {
    pg.setDirty(true);
    setVcOpen(pg.getData());
  }

  private static void setVcOpen(byte[] raw) {
    System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
  }

  public static void setVcClose(Page pg) {
    pg.setDirty(true);
    setVcClose(pg.getData());
  }

  private static void setVcClose(byte[] raw) {
    System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
  }

  /**
   * 如果是正常关闭的话，那末100～107和108～115字节的字节数组应该是一样的，如果不是的话，则说明没有正常关闭，需要进行恢复操作
   * @param pg
   * @return
   */
  public static boolean checkVc(Page pg) {
    return checkVc(pg.getData());
  }

  private static boolean checkVc(byte[] raw) {
    return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC), Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC));
  }

}
