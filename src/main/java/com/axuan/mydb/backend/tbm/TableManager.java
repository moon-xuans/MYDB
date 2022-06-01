package com.axuan.mydb.backend.tbm;

import com.axuan.mydb.backend.dm.DataManager;
import com.axuan.mydb.backend.parser.statement.Begin;
import com.axuan.mydb.backend.parser.statement.Create;
import com.axuan.mydb.backend.parser.statement.Delete;
import com.axuan.mydb.backend.parser.statement.Insert;
import com.axuan.mydb.backend.parser.statement.Select;
import com.axuan.mydb.backend.parser.statement.Update;
import com.axuan.mydb.backend.tbm.impl.TableManagerImpl;
import com.axuan.mydb.backend.utils.Parser;
import com.axuan.mydb.backend.vm.VersionManager;

/**
 * @author axuan
 * @date 2022/5/23
 **/
public interface TableManager {


  BeginRes begin(Begin begin);
  byte[] commit(long xid) throws Exception;
  byte[] abort(long xid);

  byte[] show(long xid);
  byte[] create(long xid, Create create) throws Exception;

  byte[] insert(long xid, Insert insert) throws Exception;
  byte[] read(long xid, Select select) throws Exception;
  byte[] update(long xid, Update update) throws Exception;
  byte[] delete(long xid, Delete delete) throws Exception;


  public static TableManager create(String path, VersionManager vm, DataManager dm) {
    Booter booter = Booter.create(path);
    booter.update(Parser.long2Byte(0));
    return new TableManagerImpl(vm, dm, booter);
  }



  public static TableManager open(String path, VersionManager vm, DataManager dm) {
    Booter booter = Booter.open(path);
    return new TableManagerImpl(vm, dm, booter);
  }

}
