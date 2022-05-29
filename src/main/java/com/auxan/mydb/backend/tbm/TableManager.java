package com.auxan.mydb.backend.tbm;

import com.auxan.mydb.backend.parser.statement.Begin;
import com.auxan.mydb.backend.parser.statement.Create;
import com.auxan.mydb.backend.parser.statement.Delete;
import com.auxan.mydb.backend.parser.statement.Insert;
import com.auxan.mydb.backend.parser.statement.Select;
import com.auxan.mydb.backend.parser.statement.Update;

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

}
