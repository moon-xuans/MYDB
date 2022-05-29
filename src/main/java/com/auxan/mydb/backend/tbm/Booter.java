package com.auxan.mydb.backend.tbm;

import com.auxan.mydb.backend.utils.Panic;
import com.auxan.mydb.common.Error;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * MYDB使用Booter类和bt文件，来管理MYDB的管理信息，这里只需要记录第一个表的uid
 * @author axuan
 * @date 2022/5/24
 **/
public class Booter {
  public static final String BOOTER_SUFFIX = ".bt";
  public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

  String path;
  File file;

  public static Booter open(String path) {
    removeBadTmp(path);
    File f = new File(path + BOOTER_SUFFIX);
    if (!f.exists()) {
      Panic.panic(Error.FileNotExistsException);
    }
    if (!f.canRead() || !f.canWrite()) {
      Panic.panic(Error.FileCannotRWException);
    }
    return new Booter(path ,f);
  }

  private static void removeBadTmp(String path) {
    new File(path + BOOTER_TMP_SUFFIX).delete();
  }


  private Booter(String path, File file) {
    this.path = path;
    this.file = file;
  }

  public byte[] load() {
    byte[] buf = null;
    try {
      buf = Files.readAllBytes(file.toPath());
    } catch (IOException e) {
      Panic.panic(e);
    }
    return buf;
  }

  /**
   * 这里的更新是原子的，将数据写入到一个临时文件中，然后将其名字改成xxx.bt
   * @param data
   */
  public void update(byte[] data) {
    File tmp = new File(path + BOOTER_TMP_SUFFIX);
    try {
      tmp.createNewFile();
    } catch (Exception e) {
      Panic.panic(e);
    }

    if (!tmp.canRead() || !tmp.canWrite()) {
      Panic.panic(Error.FileCannotRWException);
    }
    try (FileOutputStream out = new FileOutputStream(tmp)) {
      out.write(data);
      out.flush();
    } catch (Exception e) {
      Panic.panic(e);
    }
    try {
      Files.move(tmp.toPath(), new File(path + BOOTER_SUFFIX).toPath() , StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      Panic.panic(e);
    }
    file = new File(path + BOOTER_SUFFIX);
    if (!file.canRead() || !file.canWrite()) {
      Panic.panic(Error.FileCannotRWException);
    }
  }


}
