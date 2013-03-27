package com.epickrram.binlog;

//////////////////////////////////////////////////////////////////////////////////
//   Copyright 2011   Mark Price     mark at epickrram.com                      //
//                                                                              //
//   Licensed under the Apache License, Version 2.0 (the "License");            //
//   you may not use this file except in compliance with the License.           //
//   You may obtain a copy of the License at                                    //
//                                                                              //
//       http://www.apache.org/licenses/LICENSE-2.0                             //
//                                                                              //
//   Unless required by applicable law or agreed to in writing, software        //
//   distributed under the License is distributed on an "AS IS" BASIS,          //
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   //
//   See the License for the specific language governing permissions and        //
//   limitations under the License.                                             //
//////////////////////////////////////////////////////////////////////////////////


import com.epickrram.binlog.replay.BinaryLogRecordHandler;
import sun.misc.Unsafe;

import java.util.Date;

public enum TestLogCategory implements BinaryLogRecordHandler
{
    ONE(1),
    TWO(2),
    THREE(3);

    public static final String CATEGORY_ONE_FORMAT = "Timestamp [%s], count: %d, chars: %s, %s";
    public static final String CATEGORY_TWO_FORMAT = "Timestamp [%s], a: %.3f, b: %.3f, c: %.3f";

    private final int categoryId;

    private TestLogCategory(final int categoryId)
    {
        this.categoryId = categoryId;
    }

    @Override
    public int getCategoryId()
    {
        return categoryId;
    }

    @Override
    public String convertLogRecord(final byte[] unsafeEncodedLogRecord)
    {
        if(ONE == this)
        {
            return formatCategoryOne(unsafeEncodedLogRecord, UnsafeUtil.getUnsafe(), UnsafeUtil.getByteArrayBaseOffset());
        }
        else if(TWO == this)
        {
            return formatCategoryTwo(unsafeEncodedLogRecord, UnsafeUtil.getUnsafe(), UnsafeUtil.getByteArrayBaseOffset());
        }
        else if(THREE == this)
        {
            return formatCategoryThree(unsafeEncodedLogRecord, UnsafeUtil.getUnsafe(), UnsafeUtil.getByteArrayBaseOffset());
        }
        throw new IllegalArgumentException(String.format("Don't know how to deserialise category %s", this));
    }

    private String formatCategoryOne(final byte[] unsafeEncodedLogRecord, final Unsafe unsafe, final long byteArrayBaseOffset)
    {
        long offset = byteArrayBaseOffset;

        final long timestamp = unsafe.getLong(unsafeEncodedLogRecord, offset);
        offset += DirectMemoryLogger.SIZE_OF_LONG;
        final int counter = unsafe.getInt(unsafeEncodedLogRecord, offset);
        offset += DirectMemoryLogger.SIZE_OF_INT;
        final char asciiCharOne = (char) unsafe.getByte(unsafeEncodedLogRecord, offset++);
        final char asciiCharTwo = (char) unsafe.getByte(unsafeEncodedLogRecord, offset);

        return String.format(CATEGORY_ONE_FORMAT, new Date(timestamp),
                counter, asciiCharOne, asciiCharTwo);
    }

    private String formatCategoryTwo(final byte[] unsafeEncodedLogRecord, final Unsafe unsafe, final long byteArrayBaseOffset)
    {
        long offset = byteArrayBaseOffset;

        final long timestamp = unsafe.getLong(unsafeEncodedLogRecord, offset);
        offset += DirectMemoryLogger.SIZE_OF_LONG;
        final float f1 = unsafe.getFloat(unsafeEncodedLogRecord, offset);
        offset += DirectMemoryLogger.SIZE_OF_FLOAT;
        final float f2 = unsafe.getFloat(unsafeEncodedLogRecord, offset);
        offset += DirectMemoryLogger.SIZE_OF_FLOAT;
        final float f3 = unsafe.getFloat(unsafeEncodedLogRecord, offset);
        offset += DirectMemoryLogger.SIZE_OF_FLOAT;

        return String.format(CATEGORY_TWO_FORMAT, new Date(timestamp), f1, f2, f3);
    }

    private String formatCategoryThree(final byte[] unsafeEncodedLogRecord, final Unsafe unsafe, final long byteArrayBaseOffset)
    {
        final int value = unsafe.getInt(unsafeEncodedLogRecord, byteArrayBaseOffset);
        return Integer.toString(value);
    }
}