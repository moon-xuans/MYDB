package com.axuan.mydb.backend.tbm;


import com.axuan.mydb.backend.parser.statement.Create;
import com.axuan.mydb.backend.parser.statement.Delete;
import com.axuan.mydb.backend.parser.statement.Insert;
import com.axuan.mydb.backend.parser.statement.Select;
import com.axuan.mydb.backend.parser.statement.Update;
import com.axuan.mydb.backend.parser.statement.Where;
import com.axuan.mydb.backend.tbm.Field.ParseValueRes;
import com.axuan.mydb.backend.tbm.impl.TableManagerImpl;
import com.axuan.mydb.backend.tm.impl.TransactionManagerImpl;
import com.axuan.mydb.backend.utils.Panic;
import com.axuan.mydb.backend.utils.ParseStringRes;
import com.axuan.mydb.backend.utils.Parser;
import com.axuan.mydb.common.Error;
import com.google.common.primitives.Bytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 * @author axuan
 * @date 2022/5/23
 **/
public class Table {

  public TableManager tbm;
  public long uid;
  public String name;
  public long nextUid;
  public List<Field> fields = new ArrayList<>();

  public static Table loadTable(TableManager tbm, long uid) {
    byte[] raw = null;
    try {
      raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
    } catch (Exception e) {
      Panic.panic(e);
    }
    assert raw != null;
    Table tb = new Table(tbm, uid);
    return tb.parseSelf(raw);
  }

  public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
    Table tb = new Table(tbm, create.tableName, nextUid);
    for (int i = 0; i < create.fieldName.length; i++) {
      String fieldName = create.fieldName[i];
      String fieldType = create.fieldType[i];
      boolean indexed = false;
      for (int j = 0; j < create.index.length; j++) {
        if (fieldName.equals(create.index[j])) {
          indexed = true;
          break;
        }
      }
      tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
    }
    return tb.persistSelf(xid);
  }


  public Table(TableManager tbm, long uid) {
    this.tbm = tbm;
    this.uid = uid;
  }

  public Table(TableManager tbm, String name, long nextUid) {
    this.tbm = tbm;
    this.name = name;
    this.nextUid = nextUid;
  }



  private Table parseSelf(byte[] raw) {
    int position = 0;
    ParseStringRes res = Parser.parseString(raw);
    name = res.str;
    position += res.next;
    nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
    position += 8;

    while (position < raw.length) {
      long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
      position += 8;
      fields.add(Field.loadField(this, uid));
    }
    return this;
  }


  private Table persistSelf(long xid) throws Exception {
    byte[] nameRaw = Parser.string2Byte(name);
    byte[] nextRaw = Parser.long2Byte(nextUid);
    byte[] fieldRaw = new byte[0];
    for (Field field : fields) {
      fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
    }
    uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
    return this;
  }

  public void insert(long xid, Insert insert) throws Exception {
    Map<String, Object> entry = string2Entry(insert.values);
    byte[] raw = entry2Raw(entry);
    long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);  // 首先，将数据持久化
    for (Field field : fields) {
      if (field.isIndexed()) { // 如果属性是索引
        field.insert(entry.get(field.fieldName), uid); // 那末就应该同时构建b+数
      }
    }
  }


  private Map<String, Object> string2Entry(String[] values) throws Exception {
    if (values.length != fields.size()) {
      throw Error.InvalidValuesException;
    }
    Map<String, Object> entry = new HashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      Field f = fields.get(i);
      Object v = f.string2Value(values[i]);
      entry.put(f.fieldName, v);
    }
    return entry;
  }




  private byte[] entry2Raw(Map<String, Object> entry) {
    byte[] raw = new byte[0];
    for (Field field : fields) {
      raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
    }
    return raw;
  }

  public String read(long xid, Select read) throws Exception {
    List<Long> uids = parseWhere(read.where);
    StringBuilder sb = new StringBuilder();
    for (Long uid : uids) {
      byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid); // 根据data的uid，找到所有的数据
      if (raw == null) continue;
      Map<String, Object> entry = parseEntry(raw);
      sb.append(printEntry(entry)).append("\n");
    }
    return sb.toString();
  }

  public int update(long xid, Update update) throws Exception {
    List<Long> uids = parseWhere(update.where); // 通过where条件，以及索引计算出需要更新的uid
    Field fd = null;
    for (Field f : fields) { // 先得到哪个属性需要需要更新
      if (f.fieldName.equals(update.fieldName)) {
        fd = f;
        break;
      }
    }
    if (fd == null) {
      throw Error.FieldNotFoundException;
    }
    Object value = fd.string2Value(update.value); // 得到更新后的value值
    int count = 0;
    for (Long uid : uids) { //
      byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid); // 读到旧值
      if (raw == null) continue;

      ((TableManagerImpl)tbm).vm.delete(xid, uid); // 就旧值删除，起始就是把mMax修改成对应xid
      Map<String, Object> entry = parseEntry(raw); // 根据属性名称以及长度得到属性以及value的对应关系
      entry.put(fd.fieldName, value); // 直接覆盖
      raw = entry2Raw(entry); // 修改后的entry转成raw
      long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw); // 把修改后的数据插入

      count++;

      for (Field field : fields) { // 如果说这个更新属性是索引上的，那末需要把这个属性添加b+树
        if (field.isIndexed()) {
          field.insert(entry.get(field.fieldName), uuid);
        }
      }
    }
    return count;
  }

  public int delete(long xid, Delete delete) throws Exception {
    List<Long> uids = parseWhere(delete.where);
    int count = 0;
    for (Long uid : uids) {
      if (((TableManagerImpl)tbm).vm.delete(xid, uid)) {
        count++;
      }
    }
    return count;
  }


  class CalWhereRes {
    long l0, r0, l1, r1;
    boolean single;
  }

  /**
   * 返回所有数据的uid
   * @param where
   * @return
   * @throws Exception
   */
  public List<Long> parseWhere(Where where) throws Exception {
    long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
    boolean single = false;
    Field fd = null;
    if (where == null) { // 如果where为空，那么就找到带索引的那个属性，根据它进行寻找
      for (Field field : fields) {
        if (field.isIndexed()) {
          fd = field;
          break;
        }
      }
      l0 = 0; // 没有where，那末范围就是0～Long.MAX_VALUE
      r0 = Long.MAX_VALUE;
      single = true;
    } else {
      for (Field field : fields) {
        if (field.fieldName.equals(where.singleExp1.field)) {
          if (!field.isIndexed()) {  // 判断条件必须是索引上的
            throw Error.FieldNotIndexedException;
          }
          fd = field;
          break;
        }
      }
      if (fd == null) { // 同理，where的条件必须是索引上的，不支持全表扫描
        throw Error.FieldNotIndexedException;
      }
      CalWhereRes res = calWhere(fd, where); // 而且，这里只支持一个条件的左右where， id > 5 and id < 10类似的
      l0 = res.l0;
      r0 = res.r0;
      l1 = res.l1;
      r1 = res.r1;
      single = res.single;
    }
    List<Long> uids = fd.search(l0, r0); // 这里是通过左边界以及有边界，找到所有满足条件的uid
    if (!single) { // 如果不是单个条件，说明是两个条件，因此需要找另一对
      List<Long> tmp = fd.search(l1, r1);
      uids.addAll(tmp);
    }
    return uids;
  }


  private CalWhereRes calWhere(Field fd, Where where) throws Exception {
    CalWhereRes res = new CalWhereRes();
    switch (where.logicOp) {
      case "":
        res.single = true; // 说明它只有一个条件
        FieldCalRes r = fd.calExp(where.singleExp1);
        res.l0 = r.left;
        res.r0 = r.right;
        break;
      case "or":
        res.single = false;
        r = fd.calExp(where.singleExp1);
        res.l0 = r.left;
        res.r0 = r.right;
        r = fd.calExp(where.singleExp2);
        res.l1 = r.left;
        res.r1 = r.right;
        break;
      case "and":
        res.single = true;
        r = fd.calExp(where.singleExp1);
        res.l0 = r.left;
        res.r0 = r.right;
        r = fd.calExp(where.singleExp2);
        res.l1 = r.left;
        res.r1 = r.right;
        break;
      default:
        throw Error.InvalidLogOpException;
    }
    return res;
  }


  private String printEntry(Map<String, Object> entry) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      sb.append(field.printValue(entry.get(field.fieldName)));
      if (i == fields.size() - 1) {
        sb.append("]");
      } else {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  private Map<String, Object> parseEntry(byte[] raw) {
    int pos = 0;
    Map<String, Object> entry = new HashMap<>();
    for (Field field : fields) {
      ParseValueRes r = field.parseValue(Arrays.copyOfRange(raw, pos, raw.length));
      entry.put(field.fieldName, r.v);
      pos += r.shift;
    }
    return entry;
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("{");
    sb.append(name).append(": ");
    for (Field field : fields) {
      sb.append(field.toString());
      if (field == fields.get(fields.size() - 1)) {
        sb.append("}");
      } else {
        sb.append(",");
      }
    }
    return sb.toString();
  }
}
