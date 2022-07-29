# MYDB
## 1.前言
MYDB是一个Java实现的简单的基于文件的简易版数据库，实现了一些数据库的基本功能。
- 数据的可靠性和数据恢复
- 两段锁协议(2PL)实现可串行化调度
- MVCC
- 两种事务隔离级别(读提交和可重复读)
- 死锁处理
- 简单的表和字段管理
- 简单的SQL解析
- 基于socket的client和server。

整体上分为tm事务控制层，dm数据存储层，vm版本控制层，im索引层，tbm表结构层，transport传输层。
## 2.功能实现
### 2.1.TM(事务控制)
TM通过维护XID文件来维护事务的状态，并提供接口供其他模块来查询某个事务。
#### 2.1.1.xid文件的定义
![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111519218.png)
xid文件的前8个字节，记录事务的个数，事务xid的状态记录在(xid - 1) + 8字节处，每个事务的状态由由一个字节表示，状态分为3种，0代表活跃状态，1代表提交状态，2代表丢弃状态。
```java
  // 事务的三种状态
  private static final byte FIELD_TRAN_ACTIVE = 0;
  private static final byte FIELD_TRAN_COMMITTED = 1;
  private static final byte FIELD_TRAN_ABORTED = 2;
```
xid是从1开始的，而xid为0为超级事务，当一些操作想在没有申请事务的情况下进行，那么可以将操作的XID设置为0。XID为0的事务的状态永远是committed。

TM提供了一些接口供其他模块调用，用来创建事务和查询事务状态的:
```java
  /**开启事务*/
  long begin();

  /**提交事务*/
  void commit(long xid);

  /**丢弃事务(也可以说是回滚)*/
  void abort(long xid);

  boolean isActive(long xid);

  boolean isCommitted(long xid);

  boolean isAborted(long xid);

  /** 用于关闭文件通道和文件资源*/
  void close();
```
#### 2.1.2.实现
整体上所有的方法都是围绕xid文件进行操作的，在这里为了使文件读取更加方便使用的是NIO的FileChannel,每次开启一个事务，xidCounter就会+1，
并且会更新到文件头，来确保其数量正确，并且每次开启一个事务，都会立即刷回到磁盘，防止崩溃。
```java
  @Override
  public long begin() {
    counterLock.lock();
    try {
      long xid = xidCounter + 1;
      updateXID(xid, FIELD_TRAN_ACTIVE);
      incrXIDCounter();
      return xid;
    } finally {
     counterLock.unlock();
    }
  }
```

在创建TransactionManager时，会通过checkXidCounter来判断是否合法。校验方式的话，就是先读取文件头的
8字节来得到其事务的个数，并且可以计算出文件的长度，如果不同则认为XID文件不合法。

而事务的状态则是可以通过xid，来反推出它的位置，读取这个位置的数据，进行判断。
```java
  // 检测XID事务是否处于status状态
  private boolean checkXID(long xid, byte status) {
    long offset = getXidPosition(xid);
    ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
    try {
      fc.position(offset);
      fc.read(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    return buf.array()[0] == status;
  }

  // 根据事务xid取得其在xid文件对应的位置
  private long getXidPosition(long xid) {
    return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
  }

```
### 2.2.计数缓存和共享内存
MYDB中最底层的模块——DataManager;
> DM直接管理数据库DB文件和日志文件。DM的主要职责有:1)分页管理DB文件，并进行缓存;2)
> 管理日志文件，保证在发生错误时可以根据日志进行恢复；3)抽象DB文件为DataItem供上层使用，
> 并提供缓存。

DM的功能总结下来就是两点:上层模块和文件系统之间的一个抽象层，向下提供数据的包装；
向上提供数据的包装；另外就是日志功能。

无论是向上还是向下，DM都提供了一个缓存的功能，用内存来保证效率。

#### 2.3.1.引用计数缓存
很多地方都涉及到缓存，因此这里要涉及一个通用的缓存框架。

而这里，我使用的是引用计数缓存，一般的缓存框架都会采用LRU,实现的时候只需要
实现get(key)方法，释放的方法可以设置一个容量，当达到容量之后自动释放缓存。
但是在这里不太适用。原因呢？可以想象一个场景：当容量满了之后，缓存会自动释放一个内存。
而这个时候上层模块想要把某个资源强制刷回数据源，这个时候发现缓存中没有这个数据。
这个时候就有三种做法：
> 1.不回源。不确定释放的数据是否修改了，如果修改了，那么不回源必然会导致数据的丢失。
>
> 2.回源。回源的话，假设数据没有进行修改，那么此时回源，就白费了。
> 
> 3.放回缓存里。等下次被强制刷回数据源的时候进行回源。看起来没什么问题，但是
> 考虑到如果此时缓存已经满了，意味着必须驱逐一个资源才能放进去。这个时候就会
> 导致缓存抖动。

其实还有一种办法，就是记录数据被修改的时间，那么到下次需要刷回的时候，只需要判断
数据是否被修改即可，但是，这样的做法无非麻烦了很多。

因此，问题的根源是在，LRU缓存，资源被驱逐，上层模块无法感知。因此，可以使用引用计数
策略解决了这个问题，只有当上层模块主动释放引用，缓存在确保没有模块在使用这个资源了，
才会去驱逐资源。

引用计数的话，增加了一个方法release(key)，用于上层模块不使用某个资源时，释放
对资源的引用。当引用归零时，缓存就会驱逐这个资源，刷回到磁盘中。

如果说，缓存满了之后，引用计数无法自动释放内存，那么就应该报错。


#### 2.3.2.实现方式
**get()操作**

AbstractCache<T>是一个抽象类，内部有两个方法，留给实现类实现具体的操作，就是设计模式中的
模板模式，如果某一层想实现缓存功能，只需要实现父类的两个抽象方法即可。
```java
  /**
   * 当资源被驱逐时的写回行为
   * @param obj
   * @throws Exception
   */
  protected abstract void releaseForCache(T obj);


  /**
   * 当资源不在缓存时的获取行为
   * @param key
   * @return
   * @throws Exception
   */
  protected abstract T getForCache(long key) throws  Exception;
```
由于是引用计数，除了必需的缓存功能，也需要维护一个计数。另外，在多线程情况下，要记录
此时是否有其他线程在从数据的资源中获取。因此，需要维护这三个map。
```java
  private HashMap<Long, T> cache; // 实际缓存的数据

  private HashMap<Long, Integer> references; // 元素的引用个数

  private HashMap<Long, Boolean> getting; // 是否正在从数据库获取某资源
```

具体的获取资源的过程是这样的，首先会进入一个死循环，来无限次尝试从缓存里获取。
第一步，判断是否有其他资源正在从资源中进行获取，如果有，那么等待1s后，再过来看看。
```java
    while (true) {
      lock.lock();
      if (getting.containsKey(key)) {
        // 请求的资源正在被其他线程获取
        lock.unlock();
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
          continue;
        }
        continue;
      }
```

第二步，如果资源已经在缓存中了，那么直接获取并返回，并且给获取的资源的计数器+1.
```java
   if (cache.containsKey(key)) {
     // 资源在缓存中，直接返回
     T obj = cache.get(key);
     references.put(key, references.get(key) + 1);
     lock.unlock();
     return obj;
   }
```

第三步，如果到了这里，则说明需要从数据源中获取，在getting中记录，并count++;
```java
   // 尝试获取该资源
   if (maxResources > 0 && count == maxResources) {
     lock.unlock();
     throw Error.CacheFullException;
   }
   count++;
   getting.put(key, true);
   lock.unlock();
   break;
```

第四步，获取操作，获取成功的话，则将数据放到cache中，并从getting中移除，
并在reference中记录引用次数。
```java
   T obj = null;
   try {
     obj = getForCache(key);
   } catch (Exception e) {
     lock.lock();
     count--;
     getting.remove(key);
     lock.unlock();
     throw e;
   }

   lock.lock();
   getting.remove(key);
   cache.put(key, obj);
   references.put(key, 1);
   lock.unlock();

   return obj;
```

**release()操作**

释放操作比较简单，从reference中减1，如果减到0了，那么就需要刷回数据源，并删除缓存中
的相关结构了。
```java
  protected void release(long key){
    lock.lock();
    try {
      int ref = references.get(key) - 1;
      if (ref == 0) {
        T obj = cache.get(key);
        releaseForCache(obj);
        references.remove(key);
        cache.remove(key);
        count--;
      } else {
        references.put(key, ref);
      }
    } finally {
      lock.unlock();
    }
  }
```

缓存中，应该有一个安全关闭的功能，在关闭时，将缓存中的所有资源强行回源。
```java
 protected void close() {
   lock.lock();
   try {
     Set<Long> keys = cache.keySet();
     for (long key :keys) {
       // 这里关闭，写回资源的时候，无论是否外面引用，都会移除缓存，
       // 如果references == 0,那么就会刷回数据源；否则，直接移除。
       release(key);
       references.remove(key);
       cache.remove(key);
     }
   } finally {
     lock.unlock();
   }
 }
```

#### 2.3.3.共享内存
如果要在内存中更新数据，那么就要找到对应的位置进行修改，而java中执行类似
subArray的操作的时候，只会在底层进行一个复制，无法共用同一个内存。

因此，有一个SubArray类，来规定这个数据的可使用范围。
```java
public class SubArray {
  public byte[] raw;
  public int start;
  public int end;

  public SubArray(byte[] raw, int start, int end) {
    this.raw = raw;
    this.start = start;
    this.end = end;
  }
}
```
### 2.3.数据页的缓存与管理
这里主要是DM模块向下对文件系统的抽象部分。DM将文件系统抽象成页面，每次对
文件系统的读写都是以页面为单位的。同样，从文件系统读进来也是以页面为单位进行缓存的。

#### 2.3.1.页面缓存
这里首先要规定页面大小，和一般数据库一样，数据页大小定为8k。如果想要提升数据库
写入大量数据情况下性能的话，也可以适当增大这个值。

要实现出缓存页面，那肯定要借助之前设计的通用缓存框架。但是，要先定义出
页面的结构。注意这个页面是存储在内存中的，与已经持久化到磁盘的抽象页面
有所区别。

定义一个页面如下：
```java
public class PageImpl implements Page{


  private int pageNumber;

  private byte[] data;

  private boolean dirty;

  private Lock lock;


  // 这里Page的实现类有一个pageCache的引用，是为了方便缓存的获取和释放
  private PageCache pc;
```
其中，pageNumber为页面的页号，该页号从1开始。data就是该数据页实际包含
的字节数据。dirty标志着这个页面是否是脏页面，在缓存驱逐的时候，脏页面需要
被写回磁盘。

定义页面缓存的接口如下：
```java
public interface PageCache {

  public static final int PAGE_SIZE = 1 << 13;  // 默认页面大小为8k

  /**新建页面*/
  int newPage(byte[] initData);
  /**根据页号获取页面*/
  Page getPage(int pgNo) throws Exception;
  /**关闭页面*/
  void close();
  /**释放页面*/
  void release(Page page);


  /**设置最大页，用于截断文件*/
  void truncateByPgNo(int maxPgNo);
  /**获取页面数量*/
  int getPageNumber();
  /**刷回Page*/
  void flushPage(Page pg);
```
页面缓存的具体实现类，需要继承抽象缓存框架，并且实现`getForCache()`和
`releaseForCache()`两个抽象方法。由于这里数据源就是文件系统，`getForCache`
直接从文件中读取，并包裹成Page即可；
```java
  /**
   * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
   * @param key
   * @return
   * @throws Exception
   */
  @Override
  protected Page getForCache(long key) throws Exception {
    int pgNo = (int)key;
    long offset = PageCacheImpl.pageOffset(key);

    ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
    fileLock.lock();
    try {
      fc.position(offset);
      fc.read(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    fileLock.unlock();
    return new PageImpl(pgNo, buf.array(), this);
  }
```
而`releaseForCache()`驱逐页面时，也只需要根据页面是否是脏页，来决定
是否需要写回文件系统。
```java
 /**
   * 将脏页刷新到磁盘中
   * @param pg
   */
  @Override
  protected void releaseForCache(Page pg) {
    if (pg.isDirty()) {
      flush(pg);
      pg.setDirty(false);
    }
  }

  /**
   * 用于将页面刷新到文件磁盘
   * @param pg
   */
  private void flush(Page pg) {
    int pgNo = pg.getPageNumber();
    long offset = pageOffset(pgNo);

    fileLock.lock();
    try {
      ByteBuffer buf = ByteBuffer.wrap(pg.getData());
      fc.position(offset);
      fc.write(buf);
      fc.force(false);
    } catch (IOException e) {
      Panic.panic(e);
    } finally {
      fileLock.unlock();
    }
  }
```
从这里可以看出来，同一条数据是不允许跨页存储的。这意味着，单条数据的大小
不能超过数据库页面的大小。
#### 2.3.2.数据页管理
##### 2.3.2.1.第一页
数据库文件的第一页，通常用作一些特殊用途，比如存储一些元数据，用来启动检查
什么的。MYDB的第一页，只是用来做启动检查的。具体的原理是，在每次数据库启动时，
会生成一串随机字节，存储在100～107字节。在数据库正常关闭时，会将这串字节，拷贝
到第一页的108～115字节。
![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111529037.png)

这样数据库在每次启动时，就会检查第一页两处的字节是否相同，以此来判断上一次是否正常关闭。
如果是异常关闭，就需要执行数据的恢复流程(通过日志进行恢复).

启动时设置初始字节：
```java
  public static void setVcOpen(Page pg) {
    pg.setDirty(true);
    setVcOpen(pg.getData());
  }

  private static void setVcOpen(byte[] raw) {
    System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
  }
```
关闭时拷贝字节：
```java
  public static void setVcClose(Page pg) {
    pg.setDirty(true);
    setVcClose(pg.getData());
  }

  private static void setVcClose(byte[] raw) {
    System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
  }
```
启动时，校验字节：
```java
  /**
   * 如果是正常关闭的话，那末100～107和108～115字节的字节数组应该是一样的，如果不是的话，则说明没有正常关闭，需要进行恢复操作
   * @param pg
   * @return
   */
  public static boolean checkVc(Page pg) {
    return checkVc(pg.getData());
  }

  private static boolean checkVc(byte[] raw) {
    return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC),
                         Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC));
  }
```
##### 2.3.2.2.普通页
MYDB在普通数据页的管理比较简单。一个普通页面以一个2字节无符号数起始，表示
这一页的空闲位置的偏移。剩下的部分都是实际存储的数据。
![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111531305.png)
所以对普通页的管理，基本都是围绕对FSO(Free Space Offset)进行的。例如
向页面插入数据：
```java
  /**
   * 将raw插入pg中，返回插入位置
   * @param pg
   * @param raw
   * @return
   */
  public static short insert(Page pg, byte[] raw) {
    pg.setDirty(true);
    short offset = getFSO(pg.getData());
    System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    setFSO(pg.getData(), (short) (offset + raw.length));
    return offset;
  }

```
在写入之前获取FSO，来确定写入的位置，并在写入后更新FSO。FSO的操作如下：
```java
 /**
   * 设置空闲位置的偏移量
   * @param raw
   * @param ofData
   */
  private static void setFSO(byte[] raw, short ofData) {
    System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
  }

  /**
   * 获得该页空闲位置的偏移量
   * @param pg
   * @return
   */
  public static short getFSO(Page pg) {
    return getFSO(pg.getData());
  }

  private static short getFSO(byte[] raw) {
    return Parser.parseShort(Arrays.copyOfRange(raw, OF_FREE, OF_DATA));
  }

  /**
   * 获得页面的空闲空间大小
   * @param pg
   * @return
   */
  public static int getFreeSpace(Page pg) {
    return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
  }

```
其次，PageX中有两个需要用到的函数是`recoverInsert()`和`recoverUpdate()`
用于在数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据使用。(日志恢复中
会使用到)
```java
 /**
   * 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
   * @param pg
   * @param raw
   * @param offset
   */
  public static void recoverInsert(Page pg, byte[] raw, short offset) {
    pg.setDirty(true);
    System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

    short rawFSO = getFSO(pg.getData());
    if (rawFSO < offset + raw.length) {
      setFSO(pg.getData(), (short) (offset + raw.length));
    }
  }

  /**
   * 将raw插入pg的offset位置，不更新空闲位置的偏移量
   * @param pg
   * @param raw
   * @param offset
   */
  public static void recoverUpdate(Page pg, byte[] raw, short offset) {
    pg.setDirty(true);
    System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
  }
```
### 2.4.日志文件与恢复策略
MYDB提供了崩溃后的数据恢复功能。DM层在每次对底层数据操作时，都会记录一条日志
到磁盘上。在数据库崩溃之后，再次启动时，可以根据日志的内容，恢复数据文件，保证其一致性。
#### 2.4.1.日志读写
日志的二进制文件，按照如下的格式进行排布：
![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111532922.png)
其中XCheckNum是一个四字节的整数，是对后续所有日志计算的校验和。Log1~LogN
是常规的日志数据，BadTail是在数据库崩溃时，没有来得及写完的日志数据，这个BadTail
不一定存在。

每条日志的格式如下：

![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111532976.png)

其中，Size是一个四字节整数，标识了Data段的字节数。CheckSum则是该条日志的校验和。

单条日志的校验和，其实就是通过一个指定的种子实现的。
```java
  private int calCheckSum(int xCheck, byte[] log) {
    for (byte b : log) {
      xCheck = xCheck * SEED + b;
    }
    return xCheck;
  }
```
这样，对所有日志求出校验和，求和就能得到日志文件的校验和了。

Logger被实现成迭代器模式，通过`next()`方法，不断地从文件读出下一条日志，并将
其中的Data解析出来并返回。`next()`方法的实现主要依赖`internNext()`，大致如下，其中
position是当前日志文件读到的位置偏移。
```java
private byte[] internNext() {
    // 这个position是应该从第一条日志，也就是position=4的时候开始计算,在rewind()方法中验证了这一点
    if (position + OF_DATA >= fileSize) {
      return null;
    }
    // 读取size
    ByteBuffer tmp = ByteBuffer.allocate(4);
    try {
      fc.position(position);
      fc.read(tmp);
    } catch (IOException e) {
      Panic.panic(e);
    }
    int size = Parser.parseInt(tmp.array());
    if (position + size + OF_DATA > fileSize) {
      return null;
    }
    ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
    try {
      fc.position(position);
      fc.read(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }

    byte[] log = buf.array();
    int checkSum1 = calCheckSum(0, Arrays.copyOfRange(log, OF_DATA, log.length)); // 根据data计算出校验值
    int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA)); // 再取出日志中的校验值
    if (checkSum1 != checkSum2) {
      return null;
    }
    position += log.length;
    return log;
  }
```
在打开一个日志文件时，需要首先校验日志文件的XCheckSum,并移除文件尾部
可能存在的BadTail,由于BadTail该条日志尚未写入完成，文件的校验和也就不会
包含该日志的校验和，去掉BadTail即可保证日志文件的一致性。
```java
private void checkAndRemoveTail() {
    rewind();

    int xCheck = 0;
    while (true) {
      byte[] log = internNext();
      if (log == null) break;
      xCheck = calCheckSum(xCheck, log);
    }
    if (xCheck != xCheckSum) {
      Panic.panic(Error.BadXidFileException);
    }

    try {
      truncate(position); // 截断后面的坏的日志
    } catch (Exception e) {
      Panic.panic(e);
    }
    try {
      file.seek(position);
    } catch (IOException e) {
      Panic.panic(e);
    }
    rewind();
  }
```
向日志文件写入日志时，也是首先将数据包裹成日志格式，写入文件后，再更新
文件的校验和，更新校验和时，会刷新缓存区，保证内容写入磁盘。
```java
  public void log(byte[] data) {
   byte[] log = wrap(data);
   ByteBuffer buf = ByteBuffer.wrap(log);
   lock.lock();
   try {
     fc.position(fc.size());
     fc.write(buf);
   } catch (IOException e) {
      Panic.panic(e);
   } finally {
     lock.unlock();
   }
     updateXCheckSum(log);
  }

/**
 * 更新总的日志文件的checkSum
 * @param log
 */
  private void updateXCheckSum(byte[] log) {
   this.xCheckSum = calCheckSum(this.xCheckSum, log);
   try {
    fc.position(0);
    fc.write(ByteBuffer.wrap(Parser.int2Byte(xCheckSum)));
   } catch (IOException e) {
     Panic.panic(e);
   }
 }

  private byte[] wrap(byte[] data) {
   byte[] checkSum = Parser.int2Byte(calCheckSum(0, data));
   byte[] size = Parser.int2Byte(data.length);
   return Bytes.concat(size, checkSum, data);
  }

```
#### 2.4.2.恢复策略
DM为上层模块，提供了两种策略，分别是插入新数据(I)和更新现有数据(U)，
删除数据在VM层进行实现。

DM的日志策略：

在进行I和U操作之前，必须先进行对应的日志操作，在保证日志写入磁盘后，才能进行
数据操作。

这个日志策略，使得DM对于数据操作的磁盘同步，可以更加随意。日志在数据操作
之前，保证到达了磁盘，那么即使该数据最后没有来得及同步到磁盘，数据库
就发生了崩溃，后续也可以通过磁盘上的日志恢复该数据。

对于两种数据操作，DM记录的日志如下：
> (Ti,I,A,x),表示事务Ti在A位置插入了一条数据x
> 
> (Ti,U,A,oldx,newx),表示事务Ti将A位置的数据，从oldx更新成newx

##### 2.4.2.1.单线程
由于单线程，Ti，Tj和Tk的日志永远不会相交。这种情况日志恢复很简单，假设
日志中的最后一个事务是Ti:
> 1.对Ti之前所有的事务的日志，进行恢复
> 
> 2.接着检查Ti的状态(XID)文件，如果Ti的状态是已完成(包括committed和aborted)，
> 就将Ti重做(redo)，否则进行撤销(undo)

对事务进行redo:
> 1.正序扫描事务T的所有日志
> 
> 2.如果日志是插入操作(Ti,I,A,x)，就将x重新插入A位置
> 
> 3.如果日志是更新操作(Ti,U,A,oldx,newx)，就将A位置的值设置为newx

对事务进行undo:
> 1.倒序扫描事务T的所有日志
> 
> 2.如果日志是插入操作(Ti,I,A,x),就将A位置的数据进行删除
> 
> 3.如果日志是更新操作(Ti,U,A,oldx,newx),就将A位置的值设置为oldx

MYDB中没有真正的删除操作，对于插入操作的undo，只是将其中的标志位设置为invalid。

##### 2.4.2.2.多线程
在多线程的情况下，如果两个事务在同时进行操作，那么如果是要进行回滚，
就需要级联回滚，但是有时候committed的事务，应当被持久化，就会造成矛盾。
因此这里需要保证:
> 规定1：正在进行的事务，不会读取其他任何未提交的事务产生的数据。
> 
> 规定2：正在进行的事务，不会修改其他任何未提交的事务修改或产生的数据。


因此，出现了VM层，在MYDB中，由于VM的存在，传递到DM层，真正执行的操作
序列，都可以保护规定1和规定2。有了VM层的限制，并发情况下日志的恢复就很简单了：
> 1.重做所有崩溃时已完成(committed或aborted)的事务
> 
> 2.撤销所有崩溃未完成(active)的事务

在恢复后，数据库就会恢复到所有已完成事务结束，所有未完成事务尚未开始的状态。
##### 2.4.2.3.实现
首先规定两种日志的格式：
```java
  private static final byte LOG_TYPE_INSERT = 0; // 插入日志的标识符

  private static final byte LOG_TYPE_UPDATE = 1; // 更新日志的标识符
```
![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111536497.png)
跟原理中描述的类似，recover过程主要也是两步：重做所有已完成事务，撤销所有
未完成事务：
```java
  private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
    lg.rewind();
    while(true) {
      byte[] log = lg.next();
      if (log == null) break;
      if (isInsertLog(log)) {
          InsertLogInfo li = parseInsertLog(log);
          long xid = li.xid;
          if (!tm.isActive(xid)) {
            doInsertLog(pc, log, REDO);
          }
      } else {
        UpdateLogInfo xi = parseUpdateLog(log);
        long xid = xi.xid;
        if (!tm.isActive(xid)) {
          doUpdateLog(pc, log, REDO);
        }
      }
    }
  }

  private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
    Map<Long, List<byte[]>> logCache = new HashMap<>();
    lg.rewind();
    while (true) {
      byte[] log = lg.next();
      if (log == null) break;
   
      if (isInsertLog(log)) {
         InsertLogInfo li = parseInsertLog(log);
         long xid = li.xid;
         if (tm.isActive(xid)) {
         if (!logCache.containsKey(xid)) {
           logCache.put(xid, new ArrayList<>());
         }
        logCache.get(xid).add(log);
       }
      } else {
         UpdateLogInfo xi = parseUpdateLog(log);
         long xid = xi.xid;
         if (tm.isActive(xid)) {
         if (!logCache.containsKey(xid)) {
           logCache.put(xid, new ArrayList<>());
         }
         logCache.put(xid, new ArrayList<>());
       }
      }
    }
 
    // 对所有的active log进行倒序undo
    for (Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
      List<byte[]> logs = entry.getValue();
      for (int i = logs.size() - 1; i >= 0; i--) {
      byte[] log = logs.get(i);
      if (isInsertLog(log)) {
         doInsertLog(pc, log, UNDO);
      } else {
         doUpdateLog(pc, log, UNDO);
      }
    }
  }
 }
```

updateLog和insertLog的重做和撤销处理，分别合成一个方法来实现。
```java
  private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
    int pgNo;
    short offset;
    byte[] raw;
    if (flag == REDO) {
      UpdateLogInfo xi = parseUpdateLog(log);
      pgNo = xi.pgNo;
      offset = xi.offset;
      raw = xi.newRaw;
    } else {
      UpdateLogInfo xi = parseUpdateLog(log);
      pgNo = xi.pgNo;
      offset= xi.offset;
      raw = xi.oldRaw;
    }
    Page pg = null;
    try {
      pg = pc.getPage(pgNo);
    } catch (Exception e) {
      Panic.panic(e);
    }
    try {
      PageX.recoverUpdate(pg, raw, offset);
    } finally {
      pg.release();
    }
  }
```
```java
  private static void doInsertLog(PageCache pc, byte[] log, int flag) {
    InsertLogInfo li = parseInsertLog(log);
    Page pg = null;
    try {
      pg = pc.getPage(li.pgNo);
    } catch (Exception e) {
      Panic.panic(e);
    }
    try {
      if (flag == REDO) {
        PageX.recoverInsert(pg, li.raw, li.offset);
      } else {
        DataItem.setDataItemRawInvalid(li.raw);
      }
    } finally {
      pg.release();
    }
  }
```
注意，`doInsertLog()`方法中的删除，使用的是` DataItem.setDataItemRawInvalid(li.raw);`,
大致的作用，就是将该条DataItem的有效位设置为无效，来进行逻辑删除。
### 2.5.页面索引与DM的实现
这里是DM的最后一个环节，设计一个简单的页面索引。并且实现了DM层对于
上层的抽象：DataItem。
#### 2.5.1.页面索引
页面索引，缓存了每一页的空闲空间。用于在上层模块进行插入时，能够快速
找到一个合适空间的页面，而无需从磁盘或者缓存检查每一个页面的信息。

MYDB是将一页的空间划分成了40个区间。在启动时，就会遍历所有的页面信息，
获取页面的空闲空间，安排到这40个区间中。insert请求一个页时，会首先将所需
的空间向上取整，映射到某一个区间，随后取出这个区间的任何一页，都可以满足需求。

pageIndex的实现，就是利用一个List类型的数组。
```java
 // 将一页化成40个空间
  private static final int INTERVALS_NO = 40;
  private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

  private List<PageInfo>[] lists;
```
从PageIndex中获取页面也很简单，算出区间号，直接取出即可；
```java
  public PageInfo select(int spaceSize) {
    lock.lock();
    try {
      int number = spaceSize / THRESHOLD;
      if (number < INTERVALS_NO) number++;  // 对计算出的区间向上取整
      while (number <= INTERVALS_NO) {
        if (lists[number].size() == 0) { // 如果计算出的区间大小没有合适的，那么就加，找到更大的区间
          number++;
          continue;
        }
        return lists[number].remove(0); // 找到后，会将整个页面移出，避免并发操作，这里肯定是每个页面只被添加了一次
      }
      return null;
    } finally {
      lock.unlock();
    }
  }
```
返回的PageInfo包含页号和空闲空间大小的信息。

可以看到，被选择的页，会直接从PageIndex中移除，这意味着，同一个页面时不允许
并发写的。在上层模块使用完这个页面后，需要将其重新插入PageIndex;
```java
  public void add(int pgNo, int freeSpace) {
    lock.lock();
    try {
      int number = freeSpace / THRESHOLD;
      lists[number].add(new PageInfo(pgNo, freeSpace));
    } finally {
      lock.unlock();
    }
  }
```
在DataManager被创建时，需要获取所有页面并填充PageIndex;
```java
 // 初始化pageIndex
  void fillPageIndex() {
    int pageNumber = pc.getPageNumber();
    for (int i = 2; i <= pageNumber; i++) {
      Page pg = null;
      try {
        pg = pc.getPage(i);
      } catch (Exception e) {
        Panic.panic(e);
      }
      pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
      pg.release();
    }
  }
```
在使用完Page后需要及时release，否则可能会撑爆缓存。
#### 2.5.2.DataItem
DataItem是DM层向上层提供的数据抽象。上层模块通过地址，向DM请求到对应的
DataItem，再获取到其中的数据。

DataItem的实现：
```java
public class DataItemImpl implements DataItem {
  private SubArray raw;
  private byte[] oldRaw;  // 旧数据，和普通数据一样，包括ValidFlag/DataSize/Data
  private DataManagerImpl dm;
  private long uid;
  private Page pg;
```
保存一个dm的引用是因为其释放依赖dm的释放(dm同时实现了缓存接口，用于缓存
DataItem),以及修改数据时落地日志。

DataItem中保存的数据，结构如下：
![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111536777.png)

其中ValidFlag占用1字节，标识了该DataItem是否有效。删除一个DataItem
，只需要简单地将其有效位置设置为0。DataSize占用2字节，标识了后面Data
的长度。

上层模块在获取到DataItem后，可以通过`data()`方法，该方法返回的数组
是数据共享的，而不是拷贝实现的，所以使用了SubArray。
```java
  @Override
  public SubArray data() {
    return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
  }
```
在上层模块试图对DataItem进行修改时，需要遵循一定的流程：在修改之前需要调用
`before()`方法，想要撤销修改时，调用`unBefore()`方法，在修改完成后，调用
`after()`方法。整个流程，主要是为了保存前相数据，并及时落地日志。DM
会保证对DataItem的修改是原子性的。
```java
@Override
  public void before() {
    wLock.lock();
    pg.setDirty(true);
    System.arraycopy(raw.raw, raw.start, oldRaw,0, oldRaw.length); // 这里的拷贝，我猜测是整体上拷贝，修改一部分，也会全部拷贝
  }

  @Override
  public void unBefore() {
    System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
    wLock.unlock();
  }

  @Override
  public void after(long xid) {
    dm.logDataItem(xid, this);
    wLock.unlock();
  }
```
`after()`方法，主要就是调用dm中的一个方法，对修改操作落日志。

在使用完DataItem后，应该及时调用release()方法，释放掉DataItem的缓存
```java
  @Override
  public void release() {
    dm.releaseDataItem(this);
  }
```
#### 2.5.3.DM的实现
DataManager是DM层直接对外提供方法的类，同时，也实现成DataItem对象的
缓存。DataItem存储的key，是由页号和页内偏移组成的一个8字节无符号整数，
页号和偏移各占4字节。

DataItem缓存，`getForCache()`,只需要从key中解析出页号，从pageCache中获取
到页面，再根据偏移，解析出DataItem即可。
```java
  @Override
  protected DataItem getForCache(long uid) throws Exception {
    short offset = (short)(uid & ((1L << 16) - 1)); // offset占后两个字节
    uid >>>= 32;
    int pgNo = (int)(uid & ((1L << 32) - 1)); // pgNo占前四个字节
    Page pg = pc.getPage(pgNo);
    return DataItem.parseDataItem(pg, offset, this);
  }
```
DataItem缓存释放，需要将DataItem写回数据源，由于对文件的读写是以页为单位
进行的，只需要将DataItem所在的页release即可：
```java
  @Override
  protected void releaseForCache(DataItem di) {
    di.page().release();
  }
```
从已有文件创建DataManager和从空文件创建DataMangaer的流程稍有不同，除了
PageCache和Logger的创建方式有所不同以外，从空文件创建首先需要对第一页
进行初始化，而从已有文件创建，则是需要对第一页进行校验，来判断是否需要执行
恢复流程。并重新对第一页生成随机字节。
```java
 public static DataManager create(String path, long mem, TransactionManager tm) {
    PageCache pc = PageCache.create(path, mem);
    Logger lg = Logger.create(path);

    DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
    dm.initPageOne();
    return dm;
  }

  public static DataManager open(String path, long mem, TransactionManager tm) {
    PageCache pc = PageCache.open(path, mem);
    Logger lg = Logger.open(path);
    DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
    if (!dm.loadCheckPageOne()) {
      Recover.recover(tm, lg, pc);
    }
    dm.fillPageIndex();
    PageOne.setVcOpen(dm.pageOne);
    dm.pc.flushPage(dm.pageOne);

    return dm;
  }
```
其中，初始化第一页，和校验第一页，都是调用PageOne类中的方法实现的：
```java
// 在创建文件时初始化pageOne
  void initPageOne() {
    int pgNo = pc.newPage(PageOne.InitRaw());
    assert pgNo == 1;
    try {
      pageOne = pc.getPage(pgNo);
    } catch (Exception e) {
      Panic.panic(e);
    }
    pc.flushPage(pageOne);
  }

  // 在打开已有文件时读入PageOne，并验证正确性
  public boolean loadCheckPageOne() {
    try {
      pageOne = pc.getPage(1);
    } catch (Exception e) {
      Panic.panic(e);
    }
    return PageOne.checkVc(pageOne);
  }
```
 DM层提供了三个功能供上层使用，分别是读，插入和修改。修改是通过读出的
 DataItem实现的，于是，DataManager只需要提供`read()`和`insert()`
 方法。
 
`read()`根据UID从缓存中获取DataItem，并校验有效位：
```java
  @Override
  public DataItem read(long uid) throws Exception {
    DataItemImpl di = (DataItemImpl) super.get(uid);
    if (!di.isValid()) {
      di.release();
      return null;
    }
    return di;
  }
```
`insert()`方法，在pageIndex中获取一个足以存储插入内容的页面的页号，
获取页号后，首先需要写入插入日志，接着才可以通过PageX插入数据，并返回
插入位置的偏移量。最后需要将页面信息重新插入pageIndex.
```java
  @Override
  public long insert(long xid, byte[] data) throws Exception {
    byte[] raw = DataItem.wrapDataItemRaw(data);
    if (raw.length > PageX.MAX_FREE_SPACE) {
      throw Error.DataToolLargeException;
    }

    // 尝试获取可用页
    PageInfo pi = null;
    for (int i = 0; i < 5; i++) {
      pi = pIndex.select(raw.length);
      if (pi != null) {
        break;
      } else {
        int newPage = pc.newPage(PageX.initRaw());
        pIndex.add(newPage, PageX.MAX_FREE_SPACE);
      }
    }
    if (pi == null) {
      throw Error.DataToolLargeException;
    }

    Page pg = null;
    int freeSpace = 0;
    try {
      pg = pc.getPage(pi.pgNo);
      // 首先做日志
      byte[] log = Recover.insertLog(xid, pg, raw);
      logger.log(log);

      // 再执行插入操作
      short offset = PageX.insert(pg, raw);

      pg.release();
      return Types.addressToUid(pi.pgNo, offset);

    } finally {
      // 将取出的pg重新插入pIndex
      if (pg != null) {
        pIndex.add(pi.pgNo, PageX.getFreeSpace(pg));
      } else {
        pIndex.add(pi.pgNo, freeSpace);
      }
    }
  }
```
DataManager正常关闭时，需要执行缓存和日志的关闭流程，并且要设置第一页的
字节校验。
```java
  @Override
  public void close() {
    super.close();
    logger.close();

    PageOne.setVcClose(pageOne);
    pageOne.release();
    pc.close();
  }
```
### 2.6.记录的版本与事务隔离
> VM基于两段锁协议实现了调度序列的可串行化，并实现了MVCC以消除读写阻塞。同时
实现了两种隔离级别。

类似于DataManager是MYDB的数据管理核心，VersionManager是MYDB的事务和数据
版本的管理核心。
#### 2.6.1.2PL与MVCC
##### 2.6.1.1.冲突与2PL
数据库中的冲突，如果只考虑更新操作(U)和读操作(R)，两个操作只要满足下面
三个条件，就可以说这两个操作相互冲突：
> 1.这两个操作是由不同的事务执行
> 
> 2.这两个操作 操作的是同一个数据项
> 
> 3.这两个操作至少有一个是更新操作

那么这样，对同一个数据操作的冲突，就有两种情况:
> 1.两个不同事务的U操作冲突
>
> 2.两个不同事务的U/R操作冲突

冲突或者不冲突的影响在于，交换两个互不冲突的操作的顺序，不会对最终的结果
造成影响，而交换两个冲突操作的顺序，则是会造成影响的。

因此，VM的一个很重要的职责，就是实现了调度序列的可串行化。MYDB采用两段
锁协议(2PL)来实现。当采用2PL时，如果某个事务i已经对x加锁，且另一个事务j
也想操作x，但是这个操作与事务i之前的操作相互冲突的话，事务j就会被阻塞。
譬如，T1已经因为U1(x)锁定了x，那么T2对x的读或者写操作都会被阻塞，T2必须
等待T1释放掉对x的锁。

由此看来，2PL确实保证了调度序列的可串行化，但是不可避免地导致了事务的相互阻塞，
甚至可能导致死锁。MYDB为了提供事务处理的效率，降低阻塞概率，实现了MVCC。
##### 2.6.1.2.MVCC

DM层向上层提供了数据项(Data Item)的概念，VM通过管理所有的数据项，向上提供了记录(Entry)的概念。上层模块通过VM操作数据的最小单位，就是记录。VM在其内部，为每个记录，维护了多个版本(Version)。每当上层模块对某个记录进行修改时，VM就会为这个记录创建一个新的版本。

MYDB通过MVCC，降低了事务的阻塞概率。譬如，T1想要更新记录X的值，于是T1需要首先获取X的锁，接着更新，也就是创建了新的X的版本，假设为x3。假设T1还没有释放X的锁，T2想要读取X的值，这时候就不会阻塞，MYDB会返回一个较老版本的X，例如x2。这样最后执行的结果，就等价于T2先执行，T1后执行，调度序列依然是可串行化的。如果X没有一个更老的版本，那只能等到T1释放锁了。所以只是降低了概率。

#### 2.6.2.记录的实现

对于一条记录来说，MYDB使用Entry类维护了其结构。虽然理论上，MVCC实现了多版本，但是实现中，VM并没有提供Update操作，对于字段的更新操作由后面实现的表和字段管理(TBM)实现。所以在VM的实现中，一条记录只有一个版本。

一条记录存储在一条Data Item中，所以Entry中保存一个DataItem的引用即可。

```java
public class Entry {

  private static final int OF_XMIN = 0;
  private static final int OF_XMAX = OF_XMIN + 8;
  private static final int OF_DATA = OF_XMAX + 8;

  private long uid;
  private DataItem dataItem;
  private VersionManager vm;

  public static Entry loadEntry(VersionManager vm, long uid) throws Exception{
    DataItem di = ((VersionManagerImpl) vm).dm.read(uid);
    return newEntry(vm, di, uid);
  }    
    
  public void remove() {
    dataItem.release();
  }
}

```

我们，规定一条Entry中存储的数据格式如下：

![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111517711.png)
![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111520369.png)
XMIN是创建该条记录(版本)的事务编号，而XMAX则是删除该条记录(版本)的事务编号。Data就是这条记录持有的数据。根据这个结构，在创建记录时调用的`wrapEntryRaw()`方法如下:

```java
public static byte[] wrapEntryRaw(long xid, byte[] data) {
  byte[] xmin = Parser.long2Byte(xid);
  byte[] xmax = new byte[8];
  return Bytes.concat(xmin, xmax, data);
}
```

同样，如果要获取记录中持有的数据，也就需要按照这个结构来解析：

```java
// 以拷贝的形式返回内容
public byte[] data() {
  dataItem.rLock();
  try {
    SubArray sa = dataItem.data();
    // XMIN，XMAX也属于DataItem的一部分，不过这里只要真实数据
    byte[] data = new byte[sa.end - sa.start - OF_DATA];
    System.arraycopy(sa.raw, sa.start + OF_DATA, data, 0, data.length);
    return data;
  } finally {
    dataItem.rUnLock();
  }
}
```

这里以拷贝的形式返回数据，如果要修改的话，需要对DataItem执行`before()`方法，这个在设置XMAX的值中体现了:

```java
public void setMax(long xid) {
  dataItem.before();
  try {
    SubArray sa = dataItem.data();
    System.arraycopy(Parser.long2Byte(xid),0, sa.raw, sa.start + OF_XMAX, 8);
  } finally {
    dataItem.after(xid);
  }
}
```
#### 2.6.3.事务的隔离级别

##### 2.6.3.1.读提交

上面提到，如果一个记录的最新版本被加锁，当另一个事务想要修改或读取这条记录时，MYDB就会返回一个较旧的版本的数据。这时就可以认为，最新的被加锁的版本，对于另一个事务来说，是不可见的。这就是版本可见性。

版本的可见性与事务的隔离度是相关的。MYDB支持的最新的事务隔离程度，是“读提交”(Read Committed)，即事务在读取数据时，只能读取已经提交事务产生的数据。保证最低的读提交，就是为了防止级联回滚与commit语义冲突。

MYDB实现读提交，为每个版本维护了两个变量，就是上面提到的XMIN和XMAX：

> XMIN:创建该版本的事务编号
>
> XMAX:删除该版本的事务编号

XMIN应当在版本创建时填写，而XMAX则在版本被删除，或者有新版本出现时填写。

XMAX这个变量，也就解释了为什么DM层不提供删除操作，当想删除一个版本时，只需要设置其XMAX，这样这个版本对每一个XMAX之后的事务都是不可见的，也就等价于删除了。

因此，在读提交下，版本对事务的可见性逻辑如下：

```tex
(XMIN == Ti and // 由Ti创建且
	XMAX == null // 还未被删除
)
or 			// 或
(CMIN is committed and 	// 由一个已提交的事务创建且
	(XMAX == NULL or // 尚未删除或
	(XMAX != Ti and XMAX is not committed) // 由一个未提交的事务删除
))
```

若条件为true，则版本对Ti可见。那么获取Ti适合的版本，只需要从最新版本开始，以此向前检查可见性，如果为true，就可以直接返回。

以下方法判断某个记录对事务t是否可见:

```java
private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
  long xid = t.xid;
  long xmin = e.getXmin();
  long xmax = e.getXmax();
  if (xmin == xid && xmax == 0) return true;

  if (tm.isCommitted(xmin)) {
    if (xmax == 0) return true;
    if (xmax != xid) {
      if (!tm.isCommitted(xmax)) {
        return true;
      }
    }
  }
  return false;
}
```

这里的Transaction结构只提供了一个XID。

##### 2.6.3.2.可重复读

读提交的话，无法避免不可重复读和幻读。因此这里，有另一种隔离级别：可重复读。

我们规定：

> 事务只能读取它开始时，就已经结束的那些事务产生的数据版本

这条规定，增加于，事务需要忽略:

> 1.在本事务后开始的事务的数据；
>
> 2.本事务开始时还是active状态的事务的数据。

对于第一条，只需要比较事务ID，即可确定。而对于第二条，则需要在事务Ti开始时，记录下当前活跃的所有事务SP(Ti),如果记录的某个版本，XMIN在SP(Ti)中，也应当对Ti不可见。

于是，可重复读的逻辑判断如下:

```tex
(XMIN == Ti and  // 由Ti创建且
	(XMAX == NULL or 	// 尚未被删除
))
or 	// 或
(XMIN is committed and 	// 由一个已提交的事务创建且
XMIN < XID and  // 这个事务小于Ti且
XMIN is not in SP(Ti) and // 这个事务在Ti开始前提交且
	(XMAX == NULL or 	// 尚未被删除或
		(XMAX != Ti and  // 由其他事务删除但是
			(XMAX is not committed or // 这个事务尚未被提交或
			XMAX > Ti or // 这个事务在Ti开始之后才开始或
			XMAX is in SP(Ti) // 这个事务在Ti开始前还未被提交
))))

```

因此，需要提供一个结构，来抽象事务，以保存快照数据:

```java
public class Transaction {
  public long xid;
  public int level;
  public Map<Long, Boolean> snapshot;
  public Exception err;
  public boolean autoAborted;

  public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
    Transaction t = new Transaction();
    t.xid = xid;
    t.level = level;
    if (level != 0) {
      t.snapshot = new HashMap<>();
      for (Long x : active.keySet()) {
        t.snapshot.put(x, true);
      }
    }
    return t;
  }

  public boolean isInSnapshot(long xid) {
    if (xid == TransactionManagerImpl.SUPER_XID) {  // 如果是超级事务，直接返回false。因为超级事务默认committed
      return false;
    }
    return snapshot.containsKey(xid);
  }

}
```

构造方法中的active，保存着当前所有active的事务。于是，可重复读的隔离级别下，一个版本是否对事务可见的判断如下:

```java
private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
  long xid = t.xid;
  long xmin = e.getXmin();
  long xmax = e.getXmax();
  if (xmin == xid && xmax == 0) return true;

  if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
    if (xmax == 0) return true;
    if (xmax != xid) {
      if (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
        return true;
      }
    }
  }
  return false;
}
```
### 2.7.版本跳跃与死锁检测

这一节主要是解决MVCC可能导致的版本跳跃问题，和避免2PL导致的死锁，将其进行整合。

#### 2.7.1.版本跳跃问题

MYDB在撤销或者回滚事务：只需要将这个事务标记为aborted即可。之前对可见性的判断，每个事务只能看到其他committed的事务所产生的数据，一个aborted事务产生的数据，就不会对其他事务产生任何影响了，也就相当于，这个事务不曾存在过。

版本跳跃问题，举个例子，假设X最初只有x0版本，T1和T2都是可重复读的隔离级别:

```tex
T1 begin
T2 begin
R1(X) // T1读取X0
R2(X) // T2读取X0
U1(X) // T1将X更新到X1
T1 commit
U2(X) // T2将X更新到X2
T2 commit
```

这种情况实际运行起来是没有问题的，但是逻辑上不太正确。T1将X从X0更新为了X1，这是没错的。但是T2则是将X从X0更新成了X2，跳过了X1版本。

读提交是允许版本跳跃的，而可重复读是不允许版本跳跃的。解决版本跳跃的思路：如果Ti需要修改X,而X已经被Ti不可见的事务Tj修改了，那么要求Ti回滚。

对于Ti不可见的Tj，有两种情况:

> 1.XID(Tj) > XID(Ti)
>
> 2.Tj in SP(Ti)

于是版本跳跃的检查就是，取出要修改的数据X的最新提交版本，并检查该最新版本的创建者对当前事务是否可见:

```java
public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
  long xmax = e.getXmax();
  if (t.level == 0) {
    return false; // 如果是读已提交的情况下，则会忽略版本跳跃的问题，直接返回false即可
  } else {
    return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
  }
}
```

#### 2.7.2.死锁检测

之前提到2PL会阻塞事务，直至持有锁的线程释放锁。可以将这种等待关系抽象成有向边，例如Tj在等待Ti,就可以表示为Tj->Ti。这样，无数有向边就可以形成一个图(不一定是连通图)。检测死锁也就简单了，只需要查看这个图中是否有环即可。

MYDB使用一个LockTable对象，在内存中维护这张图。维护结构如下:

```java
public class LockTable {

  private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
  private Map<Long, Long> u2x;        // UID被某个XID持有
  private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
  private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
  private Map<Long, Long> waitU;      // XID正在等待的UID
  private Lock lock;
...
}
```

在每次出现等待的情况时，就尝试向图中增加一条边，并进行死锁检测。如果检测到死锁，就撤销这条边，不允许添加，并撤销该事务。

```java
// 不需要等待则返回null，否则返回锁对象
// 会造成死锁则抛出异常
public Lock add(long xid, long uid) throws Exception {
  lock.lock();
  try {
    if (isInList(x2u, xid, uid)) { // xid已经获得uid了，因此不需要等待
      return null;
    }
    if (!u2x.containsKey(uid)) { // 说明uid此时并没有被获取，因此可以设置之后，直接获取到，不需要等待
      u2x.put(uid, xid);
      putInfoList(x2u, xid, uid);
      return null;
    }
    waitU.put(xid, uid);  // 记录xid在等待uid
    putInfoList(wait, xid, uid); // 记录在等待uid的xid，感觉这里有点问题，xid和uid位置应该调换一下
    if (hasDeadLock()) { // 如果存在死锁，就撤销这条边，不允许添加，并撤销该事务
      waitU.remove(xid);
      removeFromList(wait, uid, xid);
      throw Error.DeadLockException;
    }
    // 这里是判断没有死锁后，并且uid已经被获取到了，因此需要等待
    // 设置xid以及对应lock，返回lock
    Lock l = new ReentrantLock();
    l.lock();  // 这里加锁返回后，要注意在selectNewXid方法里面，会打开这个锁，才能真正解锁
    waitLock.put(xid, l);
    return l;

  } finally {
    lock.unlock();
  }
}
```

调用add，如果需要等待的话，会返回一个上了锁的Lock对象。调用方在获取到该对象时，需要尝试获取该对象的锁，由此实现阻塞线程的目的，例如：

```java
l = lt.add(xid, uid); // 如果xid持有uid失败，返回lock，则进行加锁阻塞，等待其释放
if (l != null) {
  l.lock();
  l.unlock();
}
```

查找图中是否有环的算法也非常简单，就是一个深搜，需要注意这个图不一定是连通图。思路就是为每个节点设置一个访问戳，都初始化为-1，随后遍历所有节点，以每个非-1的节点作为根进行深搜，并将深搜该连通图中遇到的所有节点都设置为同一个数字，不同的连通图数字不同。这样，如果在遍历某个图时，遇到了之前遍历过的节点，说明出现了环。

```java
private boolean hasDeadLock() {
  xidStamp = new HashMap<>();
  stamp = 1;
  for (long xid : x2u.keySet()) {
    Integer s = xidStamp.get(xid);
    if (s != null && s > 0) {
      continue;
    }
    stamp++;
    if (dfs(xid)) {
      return true;
    }
  }
  return false;
}

private boolean dfs(Long xid) {
  Integer stp = xidStamp.get(xid);
  if (stp != null && stp == stamp) { // 遇到之前的标识，则说明死锁了
    return true;
  }
  if (stp != null && stp < stamp) {
    return false;
  }
  xidStamp.put(xid, stamp);

  Long uid = waitU.get(xid); // 得到xid持有的资源uid
  if (uid == null) return false; // 如果uid为null，则必不可能形成死锁
  Long x = u2x.get(uid); // 得到持有uid的xid，并继续进行遍历
  assert x != null;
  return dfs(x);
}
```

在一个事务commit或者abort时，就可以释放所有它持有的锁，并将自身从等待图中删除。

```java
  public void remove(long xid) {
    lock.lock();
    try {
      List<Long> l = x2u.get(xid);  // 获得这个xid持有的uid
      if (l != null) {
        while(l.size() > 0) {
          Long uid = l.remove(0);  // 从列表的头部开始遍历，进行分配
          selectNewXid(uid);
        }
      }
      waitU.remove(xid);  // 分配完之后，再移除waitU，说明它不再等待uid了
      x2u.remove(xid); // 移除xid
      waitLock.remove(xid); // 移除xid对应的lock
    } finally {
      lock.unlock();
    }
  }
```

while循环释放掉这个线程持有的资源的锁，这些资源可以被等待的线程所获取:

```java
  // 从等待队列中选择一个xid来占用
  private void selectNewXid(Long uid) {
    u2x.remove(uid);  // 首先解除uid和xid的绑定关系
    List<Long> l = wait.get(uid); // 获得等待uid的所有xid列表
    if (l == null) return;
    assert l.size() > 0;

    while (l.size() > 0) {
      long xid = l.remove(0); // 从xid列表的头部开始取
      if (!waitLock.containsKey(xid)) { // 则说明这个xid已经不再等待这个uid了，可能被commit/abort了
        continue;
      } else {
        u2x.put(uid, xid); // 此时uid被xid持有了
        Lock lo = waitLock.remove(xid); // 移除xid正在等待的锁
        waitU.remove(xid); // 移除xid等待uid的关系
        lo.unlock(); // 解锁
        break;
      }
    }
    if(l.size() == 0) wait.remove(uid); // 如果已经没有uid的xid了，直接移除
  }
```

从List开头开始尝试解锁，是个公平锁。解锁时，将该Lock对象unLock即可，这样业务线程就获取到了锁，就可以继续执行了。

#### 2.7.3.VM的实现

VM层通过VersionManager接口，向上层提供功能，如下：

```java
public interface VersionManager {
  byte[] read(long xid, long uid) throws Exception;
  long insert(long xid, byte[] data) throws Exception;
  boolean delete(long xid, long uid) throws Exception;

  long begin(int level);
  void commit(long xid) throws Exception;
  void abort(long xid);
}
```

同时，VM的实现类还被设计为Entry的缓存，需要继承`AbstractCache<Entry>`。需要实获取到缓存和从缓存释放的方法:

```java
@Override
protected void releaseForCache(Entry entry) {
  entry.remove();
}

@Override
protected Entry getForCache(long uid) throws Exception {
  Entry entry = Entry.loadEntry(this, uid);
  if (entry == null) {
    throw Error.NullEntryException;
  }
  return entry;
}
```

`begin()`开启一个事务，并初始化事务的结构，将其存放在activeTransaction中，用于检查和快照使用。

```java
  @Override
  public long begin(int level) {
    lock.lock();
    try {
      long xid = tm.begin();
      Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
      activeTransaction.put(xid, t);
      return xid;
    } finally {
      lock.unlock();
    }
  }
```

`commit()`方法提交一个事务，主要就是free掉相关的结构，并且释放持有的锁，并修改TM状态:

```java
  @Override
  public void commit(long xid) throws Exception {
    lock.lock();
    Transaction t = activeTransaction.get(xid);  // 通过xid获取这个事务
    lock.unlock();

    try {
      if (t.err != null) {
        throw t.err;
      }
    } catch (NullPointerException n) {
      System.out.println(xid);
      System.out.println(activeTransaction.keySet());
      Panic.panic(n);
    }

    lock.lock();
    activeTransaction.remove(xid); // 并从active事务中移除掉
    lock.unlock();

    lt.remove(xid);  // 既然这个事务已经提交，则去掉在locktable中的关联
    tm.commit(xid); // 通过tm提交这个事务
  }
```

abort事务的方法有两种，手动和自动。手动指的是调用abort()方法，而自动，则是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚。

```java
  private void internAbort(long xid, boolean autoAborted) {
    lock.lock();
    Transaction t = activeTransaction.get(xid);
    if (!autoAborted) {
      activeTransaction.remove(xid);
    }
    lock.unlock();

    if (t.autoAborted) return;
    lt.remove(xid);  // 手动的话，则需要解除locktable的一些关系
    tm.abort(xid); // 并要通过tm进行abort
  }

```

`read()`方法读取一个entry，注意判断下可见性即可：

```java
@Override
public byte[] read(long xid, long uid) throws Exception {
  lock.lock();
  Transaction t = activeTransaction.get(xid); // 读的时候先通过xid获取该事务
  lock.unlock();
  if (t.err != null) {
    throw t.err;
  }
  Entry entry = super.get(uid);  // 通过缓存获得entry
  try {
    if (Visibility.isVisible(tm,t, entry)) {
      return entry.data();
    }else{
      return null;
    }
  } finally {
    entry.release();
  }
}
```

`insert()`则是将数据包裹成Entry，交给DM插入即可：

```java
@Override
public long insert(long xid, byte[] data) throws Exception {
  lock.lock();
  Transaction t = activeTransaction.get(xid);
  lock.unlock();
  if (t.err != null) {
    throw t.err;
  }
  byte[] raw = Entry.wrapEntryRaw(xid, data);
  return dm.insert(xid, raw);
}
```

`delete()`方法稍微复杂点:

```java
@Override
public boolean delete(long xid, long uid) throws Exception {
  lock.lock();
  Transaction t = activeTransaction.get(xid); // 获得其事务
  lock.unlock();

  if (t.err != null) {
    throw t.err;
  }
  Entry entry = super.get(uid);
  try {
    if (!Visibility.isVisible(tm, t, entry)) { // 如果不满足可见性，则直接返回false
      return false;
    }
    Lock l = null;
    try {
      l = lt.add(xid, uid); // 如果xid持有uid失败，返回lock，则进行加锁阻塞，等待其释放
    } catch (Exception e) {
      t.err = Error.ConcurrentUpdateException;
      internAbort(xid, true);
      t.autoAborted = true;
      throw t.err;
    }
    if (l != null) {
      l.lock();
      l.unlock();
    }

    if (entry.getXmax() == xid) { // 如果xmax就是它本身，则重复操作，返回false
      return false;
    }

    if (Visibility.isVersionSkip(tm, t, entry)) { // 如果存在版本跳跃
      t.err = Error.ConcurrentUpdateException;
      internAbort(xid, true); // 则要自己回滚事务
      t.autoAborted = true;
      throw t.err;
    }

    entry.setMax(xid);
    return true;
  } finally {
    entry.release();
  }
}
```

实际上主要是前置的三件事:一是可见性判断，二是获取资源的锁，三是版本跳跃判断。删除的操作只有一个设置XMAX.

### 2.8.索引管理

IM,即Index Manager,索引管理器，为MYDB提供了基于B+树的聚簇索引。现在MYDB只支持索引查找数据，不支持全表扫描。

IM直接依赖于DM，而没有基于VM。索引的数据被直接插入数据库文件中，而不需要经过版本控制。

#### 2.8.1.二叉树索引

二叉树由一个个Node组成，每个Node都存储一条DataItem中。结构如下:

![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111521134.png)

其中LeafFlag标记了该节点是否是叶子节点；KeyNumber为该节点key的个数；SiblingUid是其兄弟节点存储在DM中的UID。后续是穿插的子节点(SonN)和KyeN。最后一个KeyN始终为MAX_VALUE,以此方便查找。

Node类持有了其B+树结构的引用，DataItem的引用和SubArray的引用，用于方便快速修改数据和释放数据。

```java
public class Node {
  BPlusTree tree;
  DataItem dataItem;
  SubArray raw;
  long uid;
 ...
}
```

于是生成一个根节点的数据可以写成如下:

```java
static byte[] newRootRaw(long left, long right, long key) {
    SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

    setRawIsLeaf(raw, false);
    setRawNoKeys(raw, 2);
    setRawSibling(raw, 0);
    setRawKthSon(raw, left, 0);
    setRawKthKey(raw, key, 0);
    setRawKthSon(raw, right, 1);
    setRawKthKey(raw, Long.MAX_VALUE, 1);

    return raw.raw;
}
```

该根节点的初始两个子结点为left和right，初始键值为key.

类似的，生成一个空的根节点数据

```java
static byte[] newNilRootRaw() {
  SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

  setRawIsLeaf(raw, true);
  setRawNoKeys(raw, 0);
  setRawSibling(raw, 0);

  return raw.raw;
}
```

Node类有两个方法，用于辅助B+树做插入和搜索操作，分别是searchNext和leafSearchRange方法。

searchNext寻找对应key的UID,如果找不到，则返回兄弟节点的UID.

```java
public SearchNextRes searchNext(long key) {
  dataItem.rLock();
  try {
    SearchNextRes res = new SearchNextRes();
    int noKeys = getRawNoKeys(raw);
    for (int i = 0; i < noKeys; i++) {
      long ik = getRawKthKey(raw, i);
      if (key < ik) {   // 这里我的理解是，只要找到对应的小于key的节点即可，就可以顺着索引寻找下去
        res.uid  = getRawKthSon(raw, i);
        res.siblingUid = 0;
        return res;
      }
    }
    res.uid = 0;
    res.siblingUid = getRawSibling(raw); // 若没有找到，则返回其兄弟节点
    return res;
  } finally {
    dataItem.rUnLock();
  }
}
```

leafSearchRange方法在当前节点进行范围查找，范围是[LeafKey, rightKey]，这里如果约定rightKey大于等于该节点的最大的key，则还同时返回兄弟节点的UID,方便继续搜索下一个节点。

```java
public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
  dataItem.rLock();

  try {
    int noKeys = getRawNoKeys(raw);
    int kth = 0;
    while (kth < noKeys) {
      long ik = getRawKthKey(raw, kth);
      if (ik >= leftKey) {
        break;
      }
      kth++;
    }
    ArrayList<Long> uids = new ArrayList<>();
    while (kth < noKeys) {
      long ik = getRawKthKey(raw, kth);
      if (ik <= rightKey) {
        uids.add(getRawKthSon(raw, kth));
        kth++;
      } else {
        break;
      }
    }
    long siblingUid = 0;
    if (kth == noKeys) { // 如果说寻找到头了，那末就要返回其兄弟节点，方便继续搜索下一个节点
      siblingUid = getRawSibling(raw);
    }
    LeafSearchRangeRes res = new LeafSearchRangeRes();
    res.uids = uids;
    res.siblingUid = siblingUid;
    return res;
  } finally {
    dataItem.rUnLock();
  }
}
```

由于B+树在插入删除时，会动态调整，根节点不是固定节点，于是设置一个bootDataItem，该DataItem中存储了根节点的UID。可以看到，IM在操作DM时，使用的事务都是SUPER_XID;

```java
public class BPlusTree {

  private long rootUid() {
    bootLock.lock();
    try {
      SubArray sa = bootDataItem.data();
      return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8)); // 返回data中的uid
    } finally {
      bootLock.unlock();
    }
  }


  private void updateRootUid(long left, long right, long rightKey) throws Exception {
    bootLock.lock();
    try {
      byte[] rootRaw = Node.newRootRaw(left, right, rightKey);  // 创建了一个新的根节点
      long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw); // 插入到dm中
      bootDataItem.before();
      SubArray diRaw = bootDataItem.data();
      System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8); // 将根节点的uid记录在diRaw中，相当于更新其rootUid
      bootDataItem.after(TransactionManagerImpl.SUPER_XID);
    } finally {
      bootLock.unlock();
    }
  }
}
```

IM对上层模块主要提供两种能力：插入索引和搜索节点。

但是这里可能有个问题，IM这里为什么不提供删除索引的能力。当上层模块通过VM删除某个Entry，实际的操作时设置其XMAX。如果不去删除对应索引的话，当后续再次尝试读取该Entry时，是可以通过索引寻找到的，但是由于设置了XMAX,寻找不到合适的版本而返回一个找不到内容的错误。

#### 2.8.2.可能的错误与恢复

B+树在操作过程中，可能出现两种错误，分别是节点内部错误和节点间关系错误。

当节点内部错误发生时，即当Ti在对节点的数据进行更改时，MYDB发生了崩溃。由于IM依赖于DM，在数据库重启后，Ti会被撤销，在节点的错误影响会被消除。

如果出现了节点间错误，那么一定是下面这种情况:某次对u节点的插入操作创建了新节点，此时sibling(u)=v,但是v却没有被插入到父节点中。

```tex
[parent]
    |
    v
   [u] -> [v]
```

正确的状态应当如下：

```te
[ parent ]
 |      |
 v      v
[u] -> [v]
```

这时，如果要对节点进行插入或者搜索操作，如果失败，就会继续迭代它的兄弟节点，最终还是可以找到v节点。唯一的缺点仅仅是，无法直接通过父节点找到v了，只能间接地通过u获取到v。




### 2.9.字段与表管理

TBM，表管理器。TBM实现了对字段结构和表结构的管理。同时还有MYDB使用的类SQL语句的解析。

#### 2.9.1.SQL解析器

Parser实现了对类SQL语句的结构化解析，将语句中包含的信息封装为对应语句的类。

MYDB实现的SQL语句语法如下:

```tex
<begin statement>
	begin [isolation level(read committed | repeatable read)]
	begin isolation level read committed

<commit statement>
	commit

<abort statement>
	abort
	
<create statement>
	create table <table name>
	<field name> <field type>
	<field name> <field type>	
	...
	<field name> <field type>	
	[(index <field name list>)]
	create table students
	id int32,
	name string,
	age int32
	(index id name)
	
<drop statement>
	drop table <table name>
	drop table students

<select statement>
	select (*|<field name list> from <table name> [<where statement>])
	select * from student where id = 1
	select name from student where id > 1 and id < 4
	select name, age, id from student where id = 12
	
<insert statement>
	insert into <table name> values <value list>
	insert into students values 5 "zhang san" 22
	
<delete statement>
	delete from <table name> <where statement>
	delete from student where name = "zhang san"
<update statement>
	update <table name> set <fiename> = <value> [<where statement>]
	update student set name = "zs" where id = 5
	
<where statement>
	where <field name> (>|<|=) <value> [(and|or) <field name> (>|<|=) <value>]
	
<field name> <table name>
	[a-zA-Z][a-zA-Z0-9_]*
	
<field type>
	int32 int64 string

<value>
 *
```

parser包的Tokenizer类，对语句进行逐字节解析，根据空白符或者上述词法规则，将语句切割成多个token。对外提供了`peek()`,`pop()`方法方便取出Token继续解析。

Parser类则直接对外提供了`Parse(byte[] statement)`方法，核心就是一个调用Tokeniezer类分割Token,并根据词法规则包装成具体的Statement类并返回。解析过程很简单，仅仅是根据第一个Token来区分语句类型，并分别处理。

#### 2.9.2.字段与表管理

注意，这里的字段与表管理，不是管理各个条目中不同的字段的数值等信息，而是管理表和字段的数据结构，例如表名,表字段信息和字段索引等。

由于TBM基于VM，单个字段信息和表信息都是直接保存在Entry中。字段的二进制表示如下:

![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111530581.png)

这里FieldName和TypeName,以及后面的表名，存储的都是字节形式的字符串。这里规定一个字符串的存储方式，以确定其存储边界。

![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111522374.png)

TypeName为字段的类型，限定为int32，int64和string类型。如果这个字段有索引，那个IndexUID指向了索引二叉树的根，否则该字段为0.

根据这个结构，通过一个UID从VM中读取并解析如下:

```java
  public static Field loadField(Table tb, long uid) {
    byte[] raw = null;
    try {
      raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
    } catch (Exception e) {
      Panic.panic(e);
    }
    assert raw != null;
    return new Field(uid, tb).parseSelf(raw);
  }

  private Field parseSelf(byte[] raw) {
    int position = 0;
    ParseStringRes res = Parser.parseString(raw);
    fieldName = res.str;
    position += res.next;
    res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
    fieldType = res.str;
    position += res.next;
    this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
    if (index != 0) {
      try {
        bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
      } catch (Exception e) {
        Panic.panic(e);
      }
    }
    return this;
  }
```

创建一个字段的方法类似，将相关信息通过VM持久化即可:

```java
private void persistSelf(long xid) throws Exception {
  byte[] nameRaw = Parser.string2Byte(fieldName);
  byte[] typeRaw = Parser.string2Byte(fieldType);
  byte[] indexRaw = Parser.long2Byte(index);
  this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
}
```

一张数据库有多张表，TBM使用链表的形式将其组织起来，每一张表都保存一个指向下一张表的UID。表的二进制结构如下:

这里由于每个Entry中的数据，字节数是确定的，于是无需保存字段的个数。根据UID从Entry表中读取表数据的过程和读取字段的过程类似。

对表和字段的操作，有一个很重要的步骤，就是计算Where条件的范围，目前MYDB的Where只支持两个条件的与和或。例如有条件的Delete，计算where，最终就需要获取到条件范围内所有的UID。MYDB只支持已索引字段作为where的条件。计算where的范围，具体可以看Table的`parseWhere()`和`calWhere()`方法，以及Field类的`calExp()`方法。

由于TBM的表管理，使用的是链表串起的Table结构，所以就必须保存一个链表的头节点，即第一个表的UID，这样在MYDB启动时，才能快速找到表信息。

MYDB使用Booter类和bt文件，来管理MYDB的启动过程，现在所需的启动信息，只有一个：头表的UID。Booter类对外提供了两个方法：load和upload，并保证了其原子性。update在修改bt文件内容时，没有直接对bt文件进行修改，而是首先将内容写入一个bt_tmp文件中，随后将这个文件重命名为bt文件。以此通过操作系统重命名文件的原子性，来保证操作的原子性。

```java
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
```

#### 2.9.3.TableManager

TBM层对外提供服务的是TableManager接口，如下：

```java
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
```

由于TableManager已经是直接被最外层Server调用(MYDB是C/S结构)，这些方法直接返回执行的结果，例如错误信息或者结果信息的字节数组(可读)。

各个方法的具体实现，就是调用VM的相关方法。有一点值的注意，在创建新表时，采用的是头插法，所以每次创建表都需要更新Booter文件。

### 2.10.服务端客户端的实现及其通信规则

MYDB被设计成C/S结构，类似于MySQL。支持启动一个服务器，并有多个客户端去连接，通过socket通信，执行SQL返回结果。

#### 2.10.1.C/S通信

MYDB使用了一种特殊的二进制格式。

传输的最基本结构，是Package：

```java
public class Package {
  byte[] data;
  Exception err;
}
```

每个Package在发送前，由Encoder编码为字节数组，在对方收到后同样会由Encoder解码成Package对象。编码和解码的规则如下：

![](https://raw.githubusercontent.com/moon-xuans/mediaImage/main/2022/202207111524887.png)

若flag为0，表示发送的是数据，那么data即为这份数据本身；如果flag为1，表示发送的是错误，data是Exception.getMessage()的错误提示信息。如下:

```java
public class Encoder {

  public byte[] encode(Package pkg) {
    if (pkg.getErr() != null) {
      Exception err = pkg.getErr();
      String msg = "Intern server error!";
      if (err.getMessage() != null) {
        msg = err.getMessage();
      }
      return Bytes.concat(new byte[]{1}, msg.getBytes());
    } else {
      return Bytes.concat(new byte[]{0}, pkg.getData());
    }
  }

  public Package decode(byte[] data) throws Exception {
    if (data.length < 1) {
      throw Error.InvalidPkgDataException;
    }
    if (data[0] == 0) {
      return new Package(Arrays.copyOfRange(data, 1, data.length), null);
    } else if (data[0] == 1) {
      return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
    } else {
      throw Error.InvalidPkgDataException;
    }
  }

}
```

编码之后的信息会通过Transporter类，写入输出流发送出去。为了避免特殊字符造成影响，这里会将数据转成十六进制字符串(Hex String),并为信息末尾加上换行符。这样在发送和接收数据时，就可以很简单地使用BufferedReader和Writer来直接按行读写了。

```java
public class Transporter {
  private Socket socket;
  private BufferedReader reader;
  private BufferedWriter writer;

  public Transporter(Socket socket) throws IOException {
    this.socket = socket;
    this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
  }

  public void send(byte[] data) throws IOException {
    String raw = hexEncode(data);
    writer.write(raw);
    writer.flush();
  }

  public byte[] receive() throws Exception {
    String line = reader.readLine();
    if (line == null) {
      close();
    }
    return hexDecode(line);
  }

  public void close() throws IOException {
    writer.close();
    reader.close();
    socket.close();
  }



  private String hexEncode(byte[] buf) {
    return Hex.encodeHexString(buf, true) + "\n";
  }

  private byte[] hexDecode(String buf) throws DecoderException {
    return Hex.decodeHex(buf);
  }

}
```

Packager则是Encoder和Transporter的结合体，直接对外提供send和receive方法:

```java
public class Packager {

  private Transporter transporter;
  private Encoder encoder;

  public Packager(Transporter transporter, Encoder encoder) {
    this.transporter = transporter;
    this.encoder = encoder;
  }

  public void send(Package pkg) throws IOException {
    byte[] data = encoder.encode(pkg);
    transporter.send(data);
  }

  public Package receive() throws Exception {
    byte[] data = transporter.receive();
    return encoder.decode(data);
  }

  public void close() throws IOException {
    transporter.close();
  }
}
```

#### 2.10.2.Server和Client的实现

Server启动一个ServerSocket监听端口，当有请求的到来时直接把请求丢给一个新线程处理

HandleSocket类实现了Runnable接口，在建立连接后初始化Packager，随后就循环接收来自客户端的数据并处理：

```java
Packager packager = null;
try {
  Transporter t = new Transporter(socket);
  Encoder e = new Encoder();
  packager = new Packager(t, e);
} catch (IOException e) {
  e.printStackTrace();
  try {
    socket.close();
  } catch (IOException e1) {
    e1.printStackTrace();
  }
}
Executor exe = new Executor(tbm);
while(true) {
  Package pkg = null;
  try {
    pkg = packager.receive();
  } catch (Exception e) {
    break;
  }
  byte[] sql = pkg.getData();
  byte[] result = null;
  Exception e = null;
  try {
    result = exe.execute(sql);
  } catch (Exception e1) {
    e = e1;
    e.printStackTrace();
  }
  pkg = new Package(result, e);
  try {
    packager.send(pkg);
  } catch (Exception e1) {
    e1.printStackTrace();
    break;
  }
}
exe.close();
try {
  packager.close();
} catch (Exception e) {
  e.printStackTrace();
}
```

处理的核心是Executor类，Executor调用Parser获取到对应语句的结构化信息对象，并根据对象的类型，调用TBM的不同方法进行处理。

Launcher是服务器的启动入口。这个类解析了命令行参数。有两个参数-open或者-create。Launcher根据两个参数，来决定是创建数据库文件，还是启动一个已有的数据库。

```java
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
```

客户端连接服务器的过程，通过一个简单的shell，读入用户的输入，并调用Client.execute().

```java
  public byte[] execute(byte[] stat) throws Exception {
    Package pkg = new Package(stat, null);
    Package resPkg = rt.roundTrip(pkg); // 执行后接收包
    if (resPkg.getErr() != null) { // 如果包中异常不为空，抛出异常
      throw resPkg.getErr();
    }
    return resPkg.getData(); // 没有异常的话，就返回数据
  }
```

BoundTripper类实际上实现了单次收发动作:

```java
  public Package roundTrip(Package pkg) throws Exception {
    packager.send(pkg);
    return packager.receive();
  }
```

最后附上客户端的启动入口，把Shell run起来即可。

```java
public class Launcher {

  public static void main(String[] args) throws IOException {
    Socket socket = new Socket("127.0.0.1", 9999);
    Encoder e = new Encoder();
    Transporter t = new Transporter(socket);
    Packager packager = new Packager(t, e);

    Client client = new Client(packager);
    Shell shell = new Shell(client);
    shell.run();
  }
}

```

