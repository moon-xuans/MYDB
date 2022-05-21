package com.auxan.mydb.backend.dm.dataItem;

import com.auxan.mydb.backend.dm.DataManagerImpl;
import com.auxan.mydb.backend.dm.dataItem.impl.DataItemImpl;
import com.auxan.mydb.backend.dm.page.Page;
import com.auxan.mydb.backend.utils.Parser;
import com.auxan.mydb.backend.utils.Types;
import com.auxan.mydb.common.SubArray;
import com.google.common.primitives.Bytes;
import java.util.Arrays;

/**
 * dataItem，向上提供了修改操作
 * @author axuan
 */
public interface DataItem {


    /**用于获取DataItem中的数据*/
    SubArray data();
    /**在修改之前要调用这个方法，将数据记录在oldRaw中*/
    void before();
    /**这个方法用于撤销，将oldRaw中记录的数据拷贝到原数据中即可*/
    void unBefore();
    /**在修改完成后，调用这个方法用于写入日志，因此dm保证了修改是原子性的*/
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();


    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }




    // 从页面的offset处解析dataItem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA)); // 得到的是数据的长度
        short length = (short)(size + DataItemImpl.OF_DATA); // 这个对应的是dataItem整个的长度包括 [ValidFlag] [DataSize] [Data]
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
