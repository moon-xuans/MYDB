package com.axuan.mydb.backend.dm;

import com.axuan.mydb.backend.dm.dataItem.DataItem;
import com.axuan.mydb.backend.dm.logger.Logger;
import com.axuan.mydb.backend.dm.page.Page;
import com.axuan.mydb.backend.dm.page.PageX;
import com.axuan.mydb.backend.dm.pageCache.PageCache;
import com.axuan.mydb.backend.tm.TransactionManager;
import com.axuan.mydb.backend.utils.Panic;
import com.axuan.mydb.backend.utils.Parser;
import com.axuan.mydb.backend.common.SubArray;
import com.google.common.primitives.Bytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 当数据库宕机的时候，通过日志文件来数据恢复的
 *
 * updateLog:
 * [LogType][XID][UID][OldRaw][NewRaw]
 * insertLog:
 * [LogType][XID][PgNo][Offset][Raw]
 * @author axuan
 */
public class Recover {

  // 日志的data的第一个字符有一个标识，来说明它是插入日志还是更新日志
  // 删除日志是通过更新日志完成的，直接更新为一个无效值

  private static final byte LOG_TYPE_INSERT = 0; // 插入日志的标识符

  private static final byte LOG_TYPE_UPDATE = 1; // 更新日志的标识符


  private static final int REDO = 0; // 相当于常量类，记作重做操作

  private static final int UNDO = 1; // 记作回滚操作


  /**
   * insertLog:
   * [LogType][XID][PgNo][Offset][Raw]
   */
  static class InsertLogInfo {
    long xid;
    int pgNo;
    short offset;
    byte[] raw;
  }

  static class UpdateLogInfo {
    long xid;
    int pgNo;
    short offset;
    byte[] oldRaw;
    byte[] newRaw;
  }


  // 用于数据恢复
  public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
    System.out.println("Recovering...");

    lg.rewind();
    int maxPgNo = 0;
    while (true) {
      byte[] log = lg.next();
      if (log == null) break;
      int pgNo;
      if (isInsertLog(log)) {
        InsertLogInfo li = parseInsertLog(log);
        pgNo = li.pgNo;
      } else {
        UpdateLogInfo li  = parseUpdateLog(log);
        pgNo = li.pgNo;
      }
      if (pgNo > maxPgNo) {
        maxPgNo = pgNo;
      }
    }

    if (maxPgNo == 0) {
       maxPgNo = 1;
    }
    pc.truncateByPgNo(maxPgNo);
    System.out.println("Truncate to " + maxPgNo + "pages.");

    redoTransactions(tm, lg, pc);
    System.out.println("Redo Transactions Over.");

    undoTransactions(tm, lg, pc);
    System.out.println("Undo Transactions Over.");
  }




  /**
   * 重做日志的[0]的标识符是0
   * 通过重做日志，将丢失的数据，重做
   * 步骤是：从头开始，遍历所有的日志，针对不同的日志，进行不同的操作。
   * Insert的话：是找到对应page的offset，然后将数据插入
   * Update的话:是找到对应page的offset，将newRaw放入
   * @param tm 通过事务管理器，来确认这个事务此时的状态，来进行redo或者undo
   * @param lg 通过日志，来进行获取日志，并根据log[0],来判断到底是Insert日志还是update日志
   * @param pc 通过页面缓存，将数据写入后，再刷新到磁盘
   */
  private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
    lg.rewind();
    while(true) {
      byte[] log = lg.next();
      if (log == null) break;
      if (isInsertLog(log)) {
          InsertLogInfo li = parseInsertLog(log);
          long xid = li.xid;
          if (!tm.isActive(xid)) {
            doInsertLog(pc, log, REDO);
          }
      } else {
        UpdateLogInfo xi = parseUpdateLog(log);
        long xid = xi.xid;
        if (!tm.isActive(xid)) {
          doUpdateLog(pc, log, REDO);
        }
      }
    }
  }


  /**
   * 回滚日志的[0]标识符是1
   * 这里回滚也是针对不同的操作，不过它的顺序是从尾部开始遍历
   * Insert: 会将对应page的offset，改成一个无效值
   * Update: 会将对应page的offset，改成oldRaw，也就是旧值
   * // 这里下面的逻辑都是一样的？为什么要分成两个判断。
   * // 我的理解是获得每个log之后，先通过log[0]来判断它是哪种日志，因为日志的存储方式不一样，转换的方式也不一样。
   * // 当然，这样做的目的主要还是取出xid，方便后续操作
   * @param tm
   * @param lg
   * @param pc
   */
  private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
    Map<Long, List<byte[]>> logCache = new HashMap<>();
    lg.rewind();
    while (true) {
      byte[] log = lg.next();
      if (log == null) break;

      if (isInsertLog(log)) {
        InsertLogInfo li = parseInsertLog(log);
        long xid = li.xid;
        if (tm.isActive(xid)) {
          if (!logCache.containsKey(xid)) {
            logCache.put(xid, new ArrayList<>());
          }
          logCache.get(xid).add(log);
        }
      } else {
        UpdateLogInfo xi = parseUpdateLog(log);
        long xid = xi.xid;
        if (tm.isActive(xid)) {
          if (!logCache.containsKey(xid)) {
            logCache.put(xid, new ArrayList<>());
          }
          logCache.put(xid, new ArrayList<>());
        }
      }
    }

    // 对所有的active log进行倒序undo
    for (Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
      List<byte[]> logs = entry.getValue();
      for (int i = logs.size() - 1; i >= 0; i--) {
        byte[] log = logs.get(i);
        if (isInsertLog(log)) {
          doInsertLog(pc, log, UNDO);
        } else {
          doUpdateLog(pc, log, UNDO);
        }
      }
    }
  }


  // [LogType][XID][UID][OldRaw][NewRaw]
  private static final int OF_TYPE = 0;
  private static final int OF_XID = OF_TYPE + 1; // 每个LogType占一个字节

  private static final int OF_UPDATE_UID = OF_XID + 8; // 事务XID占8个字节
  private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8; // 每个资源UID占8个字节




  public static byte[] updateLog(long xid, DataItem di) {
    byte[] logType = {LOG_TYPE_UPDATE};
    byte[] xidRaw = Parser.long2Byte(xid);
    byte[] uidRaw = Parser.long2Byte(di.getUid());
    byte[] oldRaw = di.getOldRaw();
    SubArray raw = di.getRaw();
    byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
    return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
  }



  private static UpdateLogInfo parseUpdateLog(byte[] log) {
    UpdateLogInfo li = new UpdateLogInfo();
    li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
    long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
    li.offset = (short)(uid & ((1L << 16) - 1)); // 这里，我猜测大致上uid是8个字节，后4个字节的后2个字节用来存储偏移量
    uid >>>= 32;
    li.pgNo = (int)(uid & ((1L << 32) - 1)); // uid8个字节的前4个字节，用来存储pgNo
    int length = (log.length - OF_UPDATE_RAW) / 2; // updateLog这里存储的时候，因为既存储了旧数据，也存储了新数据，并且数据的大小是一致的
    li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
    li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + 2 * length);
    return li;
  }



  private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
    int pgNo;
    short offset;
    byte[] raw;
    if (flag == REDO) {
      UpdateLogInfo xi = parseUpdateLog(log);
      pgNo = xi.pgNo;
      offset = xi.offset;
      raw = xi.newRaw;
    } else {
      UpdateLogInfo xi = parseUpdateLog(log);
      pgNo = xi.pgNo;
      offset= xi.offset;
      raw = xi.oldRaw;
    }
    Page pg = null;
    try {
      pg = pc.getPage(pgNo);
    } catch (Exception e) {
      Panic.panic(e);
    }
    try {
      PageX.recoverUpdate(pg, raw, offset);
    } finally {
      pg.release();
    }
  }




  // [LogType][XID][PgNo][Offset][Raw]
  private static final int OF_INSERT_PGNO = OF_XID + 8;
  private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4; // 页面编号占4个字节
  private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2; // 页面偏移量占2个字节

  public static byte[] insertLog(long xid, Page pg, byte[] raw) {
    byte[] logTypeRaw = {LOG_TYPE_INSERT};
    byte[] xidRaw = Parser.long2Byte(xid);
    byte[] pgNoRaw = Parser.int2Byte(pg.getPageNumber());
    byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
    return Bytes.concat(logTypeRaw, xidRaw, pgNoRaw, offsetRaw, raw);
  }


  private static InsertLogInfo parseInsertLog(byte[] log) {
    InsertLogInfo li = new InsertLogInfo();
    li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
    li.pgNo = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
    li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
    li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
    return li;
  }


  private static boolean isInsertLog(byte[] log) {
    return log[0] == LOG_TYPE_INSERT;
  }


  private static void doInsertLog(PageCache pc, byte[] log, int flag) {
    InsertLogInfo li = parseInsertLog(log);
    Page pg = null;
    try {
      pg = pc.getPage(li.pgNo);
    } catch (Exception e) {
      Panic.panic(e);
    }
    try {
      if (flag == REDO) {
        PageX.recoverInsert(pg, li.raw, li.offset);
      } else {
        DataItem.setDataItemRawInvalid(li.raw);
      }
    } finally {
      pg.release();
    }
  }



}
