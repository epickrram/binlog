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

import sun.misc.Unsafe;

/**
 * Should still only be accessed by a single thread,
 * but allows multiple threads to write to the same
 * shared memory space
 */
public final class MultiThreadedLoggingEvent implements LoggingEvent
{
    private final DirectMemoryLogger logger;
    private final LoggingCapacityAllocator capacityAllocator;
    private final byte[] buffer = new byte[1024];
    private final Unsafe unsafe;
    private final int arrayBaseOffset;

    private long positionInBuffer = 0L;
    private int logCategoryId = 0;


    public MultiThreadedLoggingEvent(final DirectMemoryLogger logger, final LoggingCapacityAllocator capacityAllocator)
    {
        this.logger = logger;
        this.capacityAllocator = capacityAllocator;
        this.unsafe = UnsafeUtil.getUnsafe();
        arrayBaseOffset = this.unsafe.arrayBaseOffset(byte[].class);
    }

    @Override
    public LoggingEvent begin(final LogCategory logCategory)
    {
        positionInBuffer = 0L;
        logCategoryId = logCategory.getCategoryId();
        return this;
    }

    @Override
    public LoggingEvent appendAsciiChar(final char value)
    {
        unsafe.putByte(buffer, arrayBaseOffset + positionInBuffer, (byte) value);
        positionInBuffer++;
        return this;
    }

    @Override
    public LoggingEvent appendByte(final byte value)
    {
        unsafe.putByte(buffer, arrayBaseOffset + positionInBuffer, value);
        positionInBuffer++;
        return this;
    }

    @Override
    public LoggingEvent appendDouble(final double value)
    {
        unsafe.putDouble(buffer, arrayBaseOffset + positionInBuffer, value);
        positionInBuffer += DirectMemoryLogger.SIZE_OF_DOUBLE;
        return this;
    }

    @Override
    public LoggingEvent appendFloat(final float value)
    {
        unsafe.putFloat(buffer, arrayBaseOffset + positionInBuffer, value);
        positionInBuffer += DirectMemoryLogger.SIZE_OF_FLOAT;
        return this;
    }

    @Override
    public LoggingEvent appendInt(final int value)
    {
        unsafe.putInt(buffer, arrayBaseOffset + positionInBuffer, value);
        positionInBuffer += DirectMemoryLogger.SIZE_OF_INT;
        return this;
    }

    @Override
    public LoggingEvent appendLong(final long value)
    {
        unsafe.putLong(buffer, arrayBaseOffset + positionInBuffer, value);
        positionInBuffer += DirectMemoryLogger.SIZE_OF_LONG;
        return this;
    }

    @Override
    public void commit()
    {
        final long totalRecordLength = positionInBuffer + 2 * DirectMemoryLogger.SIZE_OF_INT;
        final long address = capacityAllocator.getAllocatedBlockAddress(totalRecordLength);
        logger.writeIntAt(address, logCategoryId);
        logger.writeIntAt(address + DirectMemoryLogger.SIZE_OF_INT, (int) positionInBuffer);
        logger.writeByteArrayAt(address + 2 * DirectMemoryLogger.SIZE_OF_INT, buffer, (int) positionInBuffer);
    }
}
