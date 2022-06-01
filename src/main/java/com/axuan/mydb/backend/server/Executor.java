package com.axuan.mydb.backend.server;

import com.axuan.mydb.backend.parser.Parser;
import com.axuan.mydb.backend.parser.statement.Abort;
import com.axuan.mydb.backend.parser.statement.Begin;
import com.axuan.mydb.backend.parser.statement.Commit;
import com.axuan.mydb.backend.parser.statement.Create;
import com.axuan.mydb.backend.parser.statement.Delete;
import com.axuan.mydb.backend.parser.statement.Insert;
import com.axuan.mydb.backend.parser.statement.Select;
import com.axuan.mydb.backend.parser.statement.Show;
import com.axuan.mydb.backend.parser.statement.Update;
import com.axuan.mydb.backend.tbm.BeginRes;
import com.axuan.mydb.backend.tbm.TableManager;
import com.axuan.mydb.common.Error;

/**
 * @author axuan
 * @date 2022/5/29
 **/
public class Executor {
  private long xid;
  TableManager tbm;

  public Executor(TableManager tbm) {
    this.tbm = tbm;
  }

  public void close() {
    if (xid != 0) {
      System.out.println("Abnormal Abort: " + xid);
      tbm.abort(xid);
    }
  }

  public byte[] execute(byte[] sql) throws Exception {
    System.out.println("Execute: " + new String(sql));
    Object stat = Parser.Parser(sql);
    if (Begin.class.isInstance(stat)) {
      if (xid != 0) {
        throw Error.NestedTransactionException;
      }
      BeginRes r = tbm.begin((Begin) stat); // 开启一个事务，并且会将事务id赋给成员变量
      xid = r.xid;
      return r.result;
    } else if (Commit.class.isInstance(stat)) {
      if (xid == 0) {
        throw Error.NoTransactionException;
      }
      byte[] res = tbm.commit(xid);
      xid = 0;
      return res;
    } else if (Abort.class.isInstance(stat)) {
      if (xid == 0) {
        throw Error.NoTransactionException;
      }
      byte[] res = tbm.abort(xid);
      xid = 0;
      return res;
    } else {
      return execute2(stat);
    }
  }

  private byte[] execute2(Object stat) throws Exception {
    boolean tmpTransaction = false;
    Exception e = null;
    if (xid == 0) { // 如果执行的时候，此时xid=0，那末就去创建一个新的事务，用来当作临时事务
      tmpTransaction = true;
      BeginRes r = tbm.begin(new Begin());
      xid = r.xid;
    }
    try {
      byte[] res = null;
      if (Show.class.isInstance(stat)) {
        res = tbm.show(xid);
      } else if (Create.class.isInstance(stat)) {
        res = tbm.create(xid, (Create)stat);
      } else if (Select.class.isInstance(stat)) {
        res = tbm.read(xid, (Select)stat);
      } else if (Insert.class.isInstance(stat)) {
        res = tbm.insert(xid, (Insert)stat);
      } else if (Delete.class.isInstance(stat)) {
        res = tbm.delete(xid, (Delete)stat);
      } else if (Update.class.isInstance(stat)) {
        res = tbm.update(xid, (Update)stat);
      }
      return res;
    } catch (Exception e1) {
      e = e1;
      throw e;
    } finally {
      if (tmpTransaction) { // 如果使用的是临时事务，使用完之后，要根据异常的状态，进行丢弃或者删除
        if (e != null) {
          tbm.abort(xid);
        } else {
          tbm.commit(xid);
        }
        xid = 0;
      }
    }
  }
}
