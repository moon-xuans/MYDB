package com.axuan.mydb.backend.parser.statement;

/**
 * 目前只支持单个属性的更新
 * @author axuan
 * @date 2022/5/23
 **/
public class Update {
  public String tableName;
  public String fieldName;
  public String value;
  public Where where;
}
