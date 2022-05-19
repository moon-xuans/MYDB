package com.auxan.mydb.backend.dm.dataItem;

import com.auxan.mydb.backend.dm.dataItem.impl.DataItemImpl;

/**
 * @author axuan
 */
public interface DataItem {

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
