package com.axuan.mydb.backend.tbm.impl;

import com.axuan.mydb.backend.dm.DataManager;
import com.axuan.mydb.backend.parser.statement.Begin;
import com.axuan.mydb.backend.parser.statement.Create;
import com.axuan.mydb.backend.parser.statement.Delete;
import com.axuan.mydb.backend.parser.statement.Insert;
import com.axuan.mydb.backend.parser.statement.Select;
import com.axuan.mydb.backend.parser.statement.Update;
import com.axuan.mydb.backend.tbm.BeginRes;
import com.axuan.mydb.backend.tbm.Booter;
import com.axuan.mydb.backend.tbm.Table;
import com.axuan.mydb.backend.tbm.TableManager;
import com.axuan.mydb.backend.utils.Parser;
import com.axuan.mydb.backend.vm.VersionManager;
import com.axuan.mydb.common.Error;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author axuan
 * @date 2022/5/23
 **/
public class TableManagerImpl implements TableManager {

  public VersionManager vm;
  public DataManager dm;
  private Booter booter;
  private Map<String, Table> tableCache;
  private Map<Long, List<Table>> xidTableCache;
  private Lock lock;

  public TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
    this.vm = vm;
    this.dm = dm;
    this.booter = booter;
    this.tableCache = new HashMap<>();
    this.xidTableCache = new HashMap<>();
    lock = new ReentrantLock();
    loadTables();
  }

  private void loadTables() {
    long uid = firstTableUid();
    while (uid != 0) {
      Table tb = Table.loadTable(this, uid);
      uid = tb.nextUid;
      tableCache.put(tb.name, tb);
    }
  }

  /**
   * booter中记录了第一个表的uid
   * @return
   */
  private long firstTableUid() {
    byte[] raw = booter.load();
    return Parser.parseLong(raw);
  }

  /**
   * 在创建新表的时候，使用的是头插法，所以每次创建表都需要更新Booter文件
   * @param uid
   */
  private void updateFirstTableUid(long uid) {
    byte[] raw = Parser.long2Byte(uid);
    booter.update(raw);
  }

  @Override
  public BeginRes begin(Begin begin) {
    BeginRes res = new BeginRes();
    int level = begin.isRepeatAbleRead ? 1 : 0;
    res.xid = vm.begin(level);
    res.result = "begin".getBytes();
    return res;
  }

  @Override
  public byte[] commit(long xid) throws Exception {
    vm.commit(xid);
    return "commit".getBytes();
  }

  @Override
  public byte[] abort(long xid) {
    vm.abort(xid);
    return "abort".getBytes();
  }

  @Override
  public byte[] show(long xid) {
    lock.lock();
    try {
      StringBuffer sb = new StringBuffer();
      for (Table tb : tableCache.values()) {   // 从表缓存中获取所有表
        sb.append(tb.toString()).append("\n");
      }
      List<Table> t = xidTableCache.get(xid); // 从这个事务的表缓存中取出表
      if (t == null) {
        return "\n".getBytes();
      }
      for (Table tb : t) {   // sb来自两部分tableCache + xidTableCache
        sb.append(tb.toString()).append("\n");
      }
      return sb.toString().getBytes();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public byte[] create(long xid, Create create) throws Exception {
    lock.lock();
    try {
      if (tableCache.containsKey(create.tableName)) {
        throw Error.DuplicationTableException;
      }
      Table table = Table.createTable(this, firstTableUid(), xid, create);
      updateFirstTableUid(table.uid);
      tableCache.put(create.tableName, table);
      if (!xidTableCache.containsKey(xid)) {
        xidTableCache.put(xid, new ArrayList<>());
      }
      xidTableCache.get(xid).add(table);
      return ("create" + create.tableName).getBytes();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public byte[] insert(long xid, Insert insert) throws Exception {
    lock.lock();
    Table table = tableCache.get(insert.tableName);
    lock.unlock();
    if (table == null) {
      throw Error.TableNotFoundException;
    }
    table.insert(xid, insert);
    return "insert".getBytes();
  }

  @Override
  public byte[] read(long xid, Select read) throws Exception {
    lock.lock();
    Table table = tableCache.get(read.tableName);
    lock.unlock();
    if (table == null) {
      throw Error.TableNotFoundException;
    }
    return table.read(xid, read).getBytes();
  }

  @Override
  public byte[] update(long xid, Update update) throws Exception {
    lock.lock();
    Table table = tableCache.get(update.tableName);
    lock.unlock();
    if (table == null) {
      throw Error.TableNotFoundException;
    }
    int count = table.update(xid, update);
    return ("update" + count).getBytes();
  }

  @Override
  public byte[] delete(long xid, Delete delete) throws Exception {
    lock.lock();
    Table table = tableCache.get(delete.tableName);
    lock.unlock();
    if (table == null) {
      throw Error.TableNotFoundException;
    }
    int count = table.delete(xid, delete);
    return ("delete " + count).getBytes();
  }
}
