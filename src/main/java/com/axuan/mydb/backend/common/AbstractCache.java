package com.axuan.mydb.backend.common;

import com.axuan.mydb.common.Error;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 * @author axuan
 */
public abstract class AbstractCache<T> {

  private HashMap<Long, T> cache; // 实际缓存的数据

  private HashMap<Long, Integer> references; // 元素的引用个数

  private HashMap<Long, Boolean> getting; // 是否正在从数据库获取某资源


  private int maxResources; // 缓存的最大缓存资源数

  private int count = 0; // 缓存中元素的个数

  private Lock lock;

  public AbstractCache(int maxResources) {
    this.cache = new HashMap<>();
    this.references = new HashMap<>();
    this.getting = new HashMap<>();
    this.maxResources = maxResources;
    this.lock = new ReentrantLock();
  }

  protected T get(long key) throws Exception {
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

      if (cache.containsKey(key)) {
        // 资源在缓存中，直接返回
        T obj = cache.get(key);
        references.put(key, references.get(key) + 1);
        lock.unlock();
        return obj;
      }

      // 尝试获取该资源
      if (maxResources > 0 && count == maxResources) {
        lock.unlock();
        throw Error.CacheFullException;
      }
      count++;
      getting.put(key, true);
      lock.unlock();
      break;
    }

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
  }

  /**
   * 强行释放一个缓存
   * @param key
   */
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


  /**
   * 关闭缓存，写回所有资源
   */
  protected void close() {
    lock.lock();
    try {
      Set<Long> keys = cache.keySet();
      for (long key :keys) {
        // 这里关闭，写回资源的时候，无论是否外面引用，都会移除缓存，如果references == 0,那么就会刷新到数据库；否则，直接移除。
        release(key);
        references.remove(key);
        cache.remove(key);
      }
    } finally {
      lock.unlock();
    }
  }

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



}
