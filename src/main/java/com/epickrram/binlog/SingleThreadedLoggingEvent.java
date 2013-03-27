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


public final class SingleThreadedLoggingEvent implements LoggingEvent
{
    private final DirectMemoryLogger logger;
    private final LoggingCapacityAllocator capacityAllocator;

    private int length = 0;
    private long lengthFieldPosition = 0L;

    public SingleThreadedLoggingEvent(final DirectMemoryLogger logger, final LoggingCapacityAllocator capacityAllocator)
    {
        this.logger = logger;
        this.capacityAllocator = capacityAllocator;
    }

    @Override
    public LoggingEvent begin(final LogCategory logCategory)
    {
        length = 0;
        logger.writeIntAt(capacityAllocator.getAllocatedBlockAddress(DirectMemoryLogger.SIZE_OF_INT), logCategory.getCategoryId());
        lengthFieldPosition = capacityAllocator.getAllocatedBlockAddress(DirectMemoryLogger.SIZE_OF_INT);
        return this;
    }

    @Override
    public LoggingEvent appendAsciiChar(final char value)
    {
        logger.writeAsciiCharAt(capacityAllocator.getAllocatedBlockAddress(1L), value);
        length++;
        return this;
    }

    @Override
    public LoggingEvent appendByte(final byte value)
    {
        logger.writeByteAt(capacityAllocator.getAllocatedBlockAddress(1L), value);
        length++;
        return this;
    }

    @Override
    public LoggingEvent appendDouble(final double value)
    {
        logger.writeDoubleAt(capacityAllocator.getAllocatedBlockAddress(DirectMemoryLogger.SIZE_OF_DOUBLE), value);
        length += DirectMemoryLogger.SIZE_OF_DOUBLE;
        return this;
    }

    @Override
    public LoggingEvent appendFloat(final float value)
    {
        logger.writeFloatAt(capacityAllocator.getAllocatedBlockAddress(DirectMemoryLogger.SIZE_OF_FLOAT), value);
        length += DirectMemoryLogger.SIZE_OF_FLOAT;
        return this;
    }

    @Override
    public LoggingEvent appendInt(final int value)
    {
        final long allocatedBlockAddress = capacityAllocator.getAllocatedBlockAddress(DirectMemoryLogger.SIZE_OF_INT);
        logger.writeIntAt(allocatedBlockAddress, value);
        length += DirectMemoryLogger.SIZE_OF_INT;
        return this;
    }

    @Override
    public LoggingEvent appendLong(final long value)
    {
        logger.writeLongAt(capacityAllocator.getAllocatedBlockAddress(DirectMemoryLogger.SIZE_OF_LONG), value);
        length += DirectMemoryLogger.SIZE_OF_LONG;
        return this;
    }

    @Override
    public void commit()
    {
        logger.writeIntAt(lengthFieldPosition, length);
    }
}