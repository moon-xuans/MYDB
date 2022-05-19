package com.auxan.mydb.backend.dm.page;

import com.auxan.mydb.backend.dm.page.impl.PageImpl;
import com.auxan.mydb.backend.dm.pageCache.PageCache;
import com.auxan.mydb.backend.utils.Parser;
import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset][Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 * @author axuan
 */
public class PageX {
  private static final short OF_FREE = 0;

  private static final short OF_DATA = 2;

  public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;


  /**
   * 设置空闲位置的偏移量
   * @param raw
   * @param ofData
   */
  private static void setFSO(byte[] raw, short ofData) {
    System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
  }

  /**
   * 获得该页空闲位置的偏移量
   * @param pg
   * @return
   */
  public static short getFSO(Page pg) {
    return getFSO(pg.getData());
  }

  private static short getFSO(byte[] raw) {
    return Parser.parseShort(Arrays.copyOfRange(raw, OF_FREE, OF_DATA));
  }


  /**
   * 将raw插入pg中，返回插入位置
   * @param pg
   * @param raw
   * @return
   */
  public static short insert(Page pg, byte[] raw) {
    pg.setDirty(true);
    short offset = getFSO(pg.getData());
    System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    setFSO(pg.getData(), (short) (offset + raw.length));
    return offset;
  }


  /**
   * 获得页面的空闲空间大小
   * @param pg
   * @return
   */
  public static int getFreeSpace(Page pg) {
    return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
  }


  /**
   * 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
   * @param pg
   * @param raw
   * @param offset
   */
  public static void recoverInsert(Page pg, byte[] raw, short offset) {
    pg.setDirty(true);
    System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

    short rawFSO = getFSO(pg.getData());
    if (rawFSO < offset + raw.length) {
      setFSO(pg.getData(), (short) (offset + raw.length));
    }
  }

  /**
   * 将raw插入pg的offset位置，不更新空闲位置的偏移量
   * @param pg
   * @param raw
   * @param offset
   */
  public static void recoverUpdate(Page pg, byte[] raw, short offset) {
    pg.setDirty(true);
    System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
  }


}
