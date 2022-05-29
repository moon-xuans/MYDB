package com.auxan.mydb.backend.parser;

import com.auxan.mydb.backend.parser.statement.Abort;
import com.auxan.mydb.backend.parser.statement.Begin;
import com.auxan.mydb.backend.parser.statement.Commit;
import com.auxan.mydb.backend.parser.statement.Create;
import com.auxan.mydb.backend.parser.statement.Delete;
import com.auxan.mydb.backend.parser.statement.Drop;
import com.auxan.mydb.backend.parser.statement.Insert;
import com.auxan.mydb.backend.parser.statement.Select;
import com.auxan.mydb.backend.parser.statement.Show;
import com.auxan.mydb.backend.parser.statement.SingleExpression;
import com.auxan.mydb.backend.parser.statement.Update;
import com.auxan.mydb.backend.parser.statement.Where;
import com.auxan.mydb.common.Error;
import java.util.ArrayList;
import java.util.List;
import jdk.nashorn.internal.parser.Token;

/**
 * @author axuan
 * @date 2022/5/24
 **/
public class Parser {

  public static Object Parser(byte[] statement) throws Exception {
    Tokenizer tokenizer = new Tokenizer(statement);
    String token = tokenizer.peek();
    tokenizer.pop();

    Object stat = null;
    Exception statErr = null;
    try {
      switch(token) {
        case "begin":
          stat = parseBegin(tokenizer);
          break;
        case "commit":
          stat = parseCommit(tokenizer);
          break;
        case "abort":
          stat = parseAbort(tokenizer);
          break;
        case "create":
          stat = parseCreate(tokenizer);
          break;
        case "drop":
          stat = parseDrop(tokenizer);
          break;
        case "select":
          stat = parseSelect(tokenizer);
          break;
        case "insert":
          stat = parseInsert(tokenizer);
          break;
        case "delete":
          stat = parseDelete(tokenizer);
          break;
        case "update":
          stat = parseUpdate(tokenizer);
          break;
        case "show":
          stat = parseShow(tokenizer);
          break;
        default:
          throw Error.InvalidCommandException;
      }
    } catch (Exception e) {
      statErr = e;
    }
    try {
      String next = tokenizer.peek();
      if (!"".equals(next)) {
        byte[] errStat = tokenizer.errStat();
        statErr = new RuntimeException("Invalid statement: " + new String(errStat));
      }
    } catch (Exception e) {
      e.printStackTrace();
      byte[] errStat = tokenizer.errStat();
      statErr = new RuntimeException("Invalid statement: " + new String(errStat));
    }
    if (statErr != null) {
      throw statErr;
    }
    return stat;
  }

  private static Object parseShow(Tokenizer tokenizer) throws Exception {
    String tmp = tokenizer.peek();
    if ("".equals(tmp)) {
      return new Show();
    }
    throw Error.InvalidCommandException;
  }

  /**
   * 这里有限制，只能更新一个字段
   * @param tokenizer
   * @return
   * @throws Exception
   */
  private static Object parseUpdate(Tokenizer tokenizer) throws Exception {
    Update update = new Update();
    update.tableName = tokenizer.peek();
    tokenizer.pop();

    if (!"set".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    tokenizer.pop();

    update.fieldName = tokenizer.peek();
    tokenizer.pop();

    if (!"=".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    tokenizer.pop();

    update.value = tokenizer.peek();
    tokenizer.pop();

    String tmp = tokenizer.peek();
    if ("".equals(tmp)) {
      update.where = null;
      return update;
    }

    update.where = parseWhere(tokenizer);
    return update;
  }

  private static Object parseDelete(Tokenizer tokenizer) throws Exception {
    Delete delete = new Delete();

    if (!"from".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    tokenizer.pop();

    String tableName = tokenizer.peek();
    if (!isName(tableName)) {
      throw Error.InvalidCommandException;
    }
    delete.tableName = tableName;
    tokenizer.pop();

    delete.where = parseWhere(tokenizer);
    return delete;
  }

  private static Object parseInsert(Tokenizer tokenizer) throws Exception {
    Insert insert = new Insert();

    if (!"into".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    tokenizer.pop();

    String tableName = tokenizer.peek();
    if (!isName(tableName)) {
      throw Error.InvalidCommandException;
    }
    insert.tableName = tableName;
    tokenizer.pop();

    if (!"values".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }

    List<String> values = new ArrayList<>();
    while(true) {
      tokenizer.pop();;
      String value = tokenizer.peek();
      if ("".equals(value)) {
        break;
      } else {
        values.add(value);
      }
    }
    insert.values = values.toArray(new String[values.size()]);

    return insert;
  }

  private static Object parseSelect(Tokenizer tokenizer) throws Exception {
    Select read = new Select();

    List<String> fields = new ArrayList<>();
    String asterisk = tokenizer.peek();
    if ("*".equals(asterisk)) {
      fields.add(asterisk);
      tokenizer.pop();
    } else {
      while (true) {
        String field = tokenizer.peek();
        if(!isName(field)) {
          throw Error.InvalidCommandException;
        }
        fields.add(field);
        tokenizer.pop();
        if (",".equals(tokenizer.peek())) {
          tokenizer.pop();
        } else {
          break;
        }
      }
    }

    read.fields = fields.toArray(new String[fields.size()]);

    if (!"from".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    tokenizer.pop();

    String tableName = tokenizer.peek();
    if (!isName(tableName)) {
      throw Error.InvalidCommandException;
    }
    read.tableName = tableName;
    tokenizer.pop();

    String tmp = tokenizer.peek();
    if("".equals(tmp)) {
      read.where = null;
      return read;
    }

    read.where = parseWhere(tokenizer);
    return read;
  }


  private static Where parseWhere(Tokenizer tokenizer) throws Exception {
    Where where = new Where();

    if (!"where".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    tokenizer.pop();

    SingleExpression exp1 = parseSingleExp(tokenizer);
    where.singleExp1 = exp1;

    String logicOp = tokenizer.peek();
    if ("".equals(logicOp)) {
      where.logicOp = logicOp;
      return where;
    }
    if (!isLogicOp(logicOp)) {
      throw Error.InvalidCommandException;
    }
    where.logicOp = logicOp;
    tokenizer.pop();

    SingleExpression exp2 = parseSingleExp(tokenizer);
    where.singleExp2 = exp2;
    if (!"".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    return where;
  }


  /**
   * 解析成简单表达式 id > 3
   * @param tokenizer
   * @return
   * @throws Exception
   */
  private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
    SingleExpression exp = new SingleExpression();

    String field = tokenizer.peek();
    if (!isName(field)) {
      throw Error.InvalidCommandException;
    }
    exp.field = field;
    tokenizer.pop();

    String op = tokenizer.peek();
    if (!isCmpOp(op)) {
      throw Error.InvalidCommandException;
    }
    exp.compareOp = op;
    tokenizer.pop();

    exp.value = tokenizer.peek();
    tokenizer.pop();
    return exp;
  }

  /**
   * 删除表格式
   * drop table students
   * @param tokenizer
   * @return
   * @throws Exception
   */
  private static Object parseDrop(Tokenizer tokenizer) throws Exception {
    if (!"table".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    tokenizer.pop();

    String tableName = tokenizer.peek();
    if (!isName(tableName)) {
      throw Error.InvalidCommandException;
    }
    tokenizer.pop();

    if (!"".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }

    Drop drop = new Drop();
    drop.tableName = tableName;
    return drop;
  }

  /**
   * 建表格式
   * create table students
   * id int32,
   * name string,
   * age int32,
   * (index id name)
   *
   * @param tokenizer
   * @return
   * @throws Exception
   */
  private static Object parseCreate(Tokenizer tokenizer) throws Exception {
    if (!"table".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    tokenizer.pop();

    Create create = new Create();
    String name = tokenizer.peek();
    if (!isName(name)) {
      throw Error.InvalidCommandException;
    }
    create.tableName = name;

    List<String> fNames = new ArrayList<>();
    List<String> fTypes = new ArrayList<>();
    while(true) {
      tokenizer.pop();
      String field = tokenizer.peek();
      if ("(".equals(field)) {
        break;
      }

      if (!isName(field)) {
        throw Error.InvalidCommandException;
      }

      tokenizer.pop();
      String fieldType = tokenizer.peek();
      if(!isType(fieldType)) {
        throw Error.InvalidCommandException;
      }
      fNames.add(field);
      fTypes.add(fieldType);
      tokenizer.pop();

      String next = tokenizer.peek();
      if (",".equals(next)) {
        continue;
      } else if ("".equals(next)) {
        throw Error.TableNoIndexException;
      } else if ("(".equals(next)) {
        break;
      } else {
        throw Error.InvalidCommandException;
      }
    }
    create.fieldName = fNames.toArray(new String[fNames.size()]);
    create.fieldType = fTypes.toArray(new String[fTypes.size()]);

    tokenizer.pop();
    if (!"index".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }

    List<String> indexes = new ArrayList<>();
    while (true) {
      tokenizer.pop();
      String field = tokenizer.peek();
      if (")".equals(field)) {
        break;
      }
      if (!isName(field)) {
        throw Error.InvalidCommandException;
      } else {
        indexes.add(field);
      }
    }

    create.index = indexes.toArray(new String[indexes.size()]);
    tokenizer.pop();

    if (!"".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    return create;
  }

  /**
   *  abort
   * @param tokenizer
   * @return
   * @throws Exception
   */
  private static Object parseAbort(Tokenizer tokenizer) throws Exception {
    if (!"".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    return new Abort();
  }

  /**
   * commit
   * @param tokenizer
   * @return
   * @throws Exception
   */
  private static Object parseCommit(Tokenizer tokenizer) throws Exception {
    if (!"".equals(tokenizer.peek())) {
      throw Error.InvalidCommandException;
    }
    return new Commit();
  }

  /**
   * 开启事务格式
   * begin isolation level read committed
   * @param tokenizer
   * @return
   * @throws Exception
   */
  private static Object parseBegin(Tokenizer tokenizer) throws Exception {
    String isolation = tokenizer.peek();
    Begin begin = new Begin();
    if ("".equals(isolation)) {
      return begin;
    }
    if (!"isolation".equals(isolation)) {
      throw Error.InvalidCommandException;
    }

    tokenizer.pop();;
    String level = tokenizer.peek();
    if (!"level".equals(level)) {
      throw Error.InvalidCommandException;
    }
    tokenizer.pop();

    String tmp1 = tokenizer.peek();
    if ("read".equals(tmp1)) {
      tokenizer.pop();
      String tmp2 = tokenizer.peek();
      if ("committed".equals(tmp2)) {
        tokenizer.pop();
        if (!"".equals(tokenizer.peek())) { // 如果后面还有其他字符串，那末就是错误命令
          throw Error.InvalidCommandException;
        }
        return begin;
      } else {
        throw Error.InvalidCommandException;
      }
    } else if ("repeatable".equals(tmp1)) {
      tokenizer.pop();
      String tmp2 = tokenizer.peek();
      if ("read".equals(tmp2)) {
        begin.isRepeatAbleRead = true;
        tokenizer.pop();
        if (!"".equals(tokenizer.peek())) { // 同理，如果后面，还有其他字符串，则是个错误命令
          throw Error.InvalidCommandException;
        }
        return begin;
      } else {
        throw Error.InvalidCommandException;
      }
    } else {
      throw Error.InvalidCommandException;
    }
  }

  private static boolean isType(String tp) {
    return ("int32".equals(tp) || "int64".equals(tp) ||
        "string".equals(tp));
  }

  private static boolean isName(String name) {
    return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
  }

  private static boolean isCmpOp(String op) {
    return ("=".equals(op) || ">".equals(op) || "<".equals(op));
  }

  private static boolean isLogicOp(String op) {
    return ("and".equals(op) || "or".equals(op));
  }
}
