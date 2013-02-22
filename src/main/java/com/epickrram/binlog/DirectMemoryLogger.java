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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.lang.String.format;

public final class DirectMemoryLogger
{
    public static final int SIZE_OF_DOUBLE = 8;
    public static final int SIZE_OF_FLOAT = 4;
    public static final int SIZE_OF_LONG = 8;
    public static final int SIZE_OF_INT = 4;
    public static final int SIZE_OF_BYTE = 1;

    private final long bufferAddress;
    private final int pageSize;
    private final Unsafe unsafe;
    private final long dataOffset;
    private final MappedByteBuffer buffer;
    private final RandomAccessFile file;
    private final long bufferLength;

    public DirectMemoryLogger(final String filename, final long bufferLength)
            throws IOException, NoSuchFieldException, IllegalAccessException
    {
        file = new RandomAccessFile(filename, "rw");
        final FileChannel fileChannel = file.getChannel();
        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, bufferLength);

        final Field address = Buffer.class.getDeclaredField("address");
        unsafe = UnsafeUtil.getUnsafe();
        final long addressFieldOffset = unsafe.objectFieldOffset(address);
        bufferAddress = unsafe.getLong(buffer, addressFieldOffset);
        pageSize = unsafe.pageSize();
        dataOffset = getDataOffset(mappingOffset());
        this.bufferLength = bufferLength;
    }

    public DirectMemoryLogger writeAsciiCharAt(final long position, final char value)
    {
        return writeByteAt(position, (byte) value);
    }

    public DirectMemoryLogger writeByteAt(final long position, final byte value)
    {
        assert boundsCheck(position, SIZE_OF_BYTE) : format("Tried to write byte at %d, buffer length is %d", position, bufferLength);

        unsafe.putByte(getPointer(position), value);

        return this;
    }

    public DirectMemoryLogger writeDoubleAt(final long position, final double value)
    {
        assert boundsCheck(position, SIZE_OF_DOUBLE) : format("Tried to write double at %d, buffer length is %d", position, bufferLength);

        unsafe.putDouble(getPointer(position), value);

        return this;
    }

    public DirectMemoryLogger writeFloatAt(final long position, final float value)
    {
        assert boundsCheck(position, SIZE_OF_FLOAT) : format("Tried to write float at %d, buffer length is %d", position, bufferLength);

        unsafe.putFloat(getPointer(position), value);

        return this;
    }

    public DirectMemoryLogger writeIntAt(final long position, final int value)
    {
        assert boundsCheck(position, SIZE_OF_INT) : format("Tried to write int at %d, buffer length is %d", position, bufferLength);

        unsafe.putInt(getPointer(position), value);

        return this;
    }

    public DirectMemoryLogger writeLongAt(final long position, final long value)
    {
        assert boundsCheck(position, SIZE_OF_LONG) : format("Tried to write long at %d, buffer length is %d", position, bufferLength);

        unsafe.putLong(getPointer(position), value);

        return this;
    }

    DirectMemoryLogger writeByteArrayAt(final long position, final byte[] value, final int length)
    {
        for(int i = 0; i < length; i++)
        {
            writeByteAt(position + i, value[i]);
        }

        return this;
    }

    public DirectMemoryLogger flush()
    {
        buffer.force();

        return this;
    }

    public void close() throws IOException
    {
        file.close();
    }

    private boolean boundsCheck(final long position, final int sizeOfDataItem)
    {
        return position < bufferLength - sizeOfDataItem;
    }

    private long getPointer(final long position)
    {
        return dataOffset + position;
    }

    private long mappingOffset()
    {
        final long offset = bufferAddress % pageSize;
        return (offset >= 0) ? offset : (pageSize + offset);
    }

    private long getDataOffset(long mappingOffset)
    {
        return bufferAddress - mappingOffset;
    }
}