package com.axuan.mydb.backend;

import com.axuan.mydb.backend.dm.DataManager;
import com.axuan.mydb.backend.server.Server;
import com.axuan.mydb.backend.tbm.TableManager;
import com.axuan.mydb.backend.tm.TransactionManager;
import com.axuan.mydb.backend.tm.impl.TransactionManagerImpl;
import com.axuan.mydb.backend.utils.Panic;
import com.axuan.mydb.backend.vm.VersionManager;
import com.axuan.mydb.backend.vm.impl.VersionManagerImpl;
import com.axuan.mydb.common.Error;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * @author axuan
 * @date 2022/5/29
 **/
public class Launcher {
  public static final int port = 9999;

  public static final long DEFAULT_MEM = (1 << 20) * 64; // 默认是64k
  public static final long KB = 1 << 10; // 单位
  public static final long MB = 1 << 20;
  public static final long GB = 1 << 30;

  public static void main(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption("open", true, "-open DBPath");
    options.addOption("create", true, "-create DBPath");
    options.addOption("mem", true, "-mem 64MB");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("open")) {
      openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
      return;
    }
    if (cmd.hasOption("create")) {
      createDB(cmd.getOptionValue("create"));
      return;
    }
    System.out.println("Usage: Launcher (open|create) DBPath");
  }

  private static void createDB(String path) {
    TransactionManagerImpl tm = TransactionManager.create(path);
    DataManager dm = DataManager.create(path, DEFAULT_MEM, tm);
    VersionManagerImpl vm = new VersionManagerImpl(tm, dm);
    TableManager.create(path, vm, dm);
    tm.close();
    dm.close();
  }

  private static void openDB(String path, long mem) {
    TransactionManager tm = TransactionManager.open(path);
    DataManager dm = DataManager.open(path, mem, tm);
    VersionManager vm = new VersionManagerImpl(tm, dm);
    TableManager tbm = TableManager.open(path, vm, dm);
    new Server(port, tbm).start();
  }

  private static long parseMem(String memStr) {
    if (memStr == null || "".equals(memStr)) {
      return DEFAULT_MEM;
    }
    if (memStr.length() < 2) {
      Panic.panic(Error.InvalidMemException);
    }
    String unit = memStr.substring(memStr.length() - 2);// 截取单位
    long memNum = Long.parseLong(memStr.substring(0, memStr.length() - 2));
    switch (unit) {
      case "KB":
        return memNum * KB;
      case "MB":
        return memNum * MB;
      case "GB":
        return memNum * GB;
      default:
        Panic.panic(Error.InvalidMemException);
    }
    return DEFAULT_MEM;
  }
}
