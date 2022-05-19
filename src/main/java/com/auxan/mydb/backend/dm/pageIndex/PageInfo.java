package com.auxan.mydb.backend.dm.pageIndex;

/**
 * pageInfo，一个info，用来形容这个页的页码以及空间空间
 * @author axuan
 */
public class PageInfo {

  public int pgNo;
  public int freeSpace;

  public PageInfo(int pgNo, int freeSpace) {
    this.pgNo = pgNo;
    this.freeSpace = freeSpace;
  }
}
