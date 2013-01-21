package com.epickrram.binlog.replay;

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


import com.epickrram.binlog.DirectMemoryLogger;
import com.epickrram.binlog.UnsafeUtil;
import sun.misc.Unsafe;

public final class LogReplay
{
    private final BinaryLogRecordHandlerRegistry registry;
    private final byte[] data;

    public LogReplay(final BinaryLogRecordHandlerRegistry registry, final byte[] data)
    {
        this.registry = registry;
        this.data = data;
    }

    public void replay(final LogRecordHandler handler)
    {
        final Unsafe unsafe = UnsafeUtil.getUnsafe();
        final long byteArrayBaseOffset = UnsafeUtil.getByteArrayBaseOffset();

        long offset = 0L;

        while(offset < data.length)
        {
            final int logCategoryId = unsafe.getInt(data, byteArrayBaseOffset + offset);
            offset += DirectMemoryLogger.SIZE_OF_INT;

            if(logCategoryId == 0)
            {
                break;
            }

            final int length = unsafe.getInt(data, byteArrayBaseOffset + offset);
            offset += DirectMemoryLogger.SIZE_OF_INT;

            final byte[] buffer = new byte[length];
            for(int i = 0; i < length; i++)
            {
                buffer[i] = unsafe.getByte(data, byteArrayBaseOffset + offset++);
            }

            handler.onLogRecord(registry.getByCategoryId(logCategoryId).convertLogRecord(buffer));
        }
    }
}