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


import com.epickrram.binlog.replay.BinaryLogRecordHandlerRegistry;
import com.epickrram.binlog.replay.LogRecordHandler;
import com.epickrram.binlog.replay.LogReplay;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import sun.misc.Unsafe;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public final class DirectMemoryLoggerIntegrationTest
{
    private static final String FILENAME = System.getProperty("java.io.tmpdir") +
            File.separator + System.getProperty("user.name") + "-" + System.currentTimeMillis();

    private static final int ONE_KILOBYTE = 1024;
    private static final char ASCII_CHAR = 'h';
    private static final byte BYTE = (byte) 42;
    private static final int INT = 123456789;
    private static final long LONG = Long.MIN_VALUE;
    private static final float FLOAT = 3.14f;
    private static final double DOUBLE = 123456789.123456789d;

    private DirectMemoryLogger logger;
    private Unsafe unsafe;
    private long byteArrayOffset;

    @Before
    public void setup() throws Exception
    {
        new File(FILENAME).delete();
        logger = new DirectMemoryLogger(FILENAME, ONE_KILOBYTE);
        unsafe = UnsafeUtil.getUnsafe();
        byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);
    }

    @Ignore
    @Test
    public void perftest() throws Exception
    {
        System.out.println(String.format("Start of test, mem free: %d/%d",
                Runtime.getRuntime().freeMemory(), Runtime.getRuntime().totalMemory()));
        final int fileSize = 1024 * 1024 * 1024;
        logger = new DirectMemoryLogger(FILENAME + "-1", fileSize);

        LoggingService loggingService = new LoggingService(logger);
        LoggingEvent loggingEvent = loggingService.singleThreadedAccess().begin(TestLogCategory.ONE);
        // long/int comparison

        System.out.println(String.format("Logger created, mem free: %d/%d",
                Runtime.getRuntime().freeMemory(), Runtime.getRuntime().totalMemory()));

        final int intCount = (fileSize / 2 - 8192) / DirectMemoryLogger.SIZE_OF_INT;
        final int longCount = (fileSize / 2 - 8192) / DirectMemoryLogger.SIZE_OF_LONG;


        System.out.println(intCount);
        System.out.println(longCount);
        long start;



        start = System.nanoTime();
        for(int i = 0; i < longCount; i++)
        {
//            if(i % 100000 == 0)
//            {
//                System.out.println(i);
//            }
            loggingEvent.appendLong(i);
        }
        loggingEvent.commit();
        log(longCount, start, "long");

        start = System.nanoTime();
        for(int i = 0; i < intCount; i++)
        {
            loggingEvent.appendInt(i);
        }
        loggingEvent.commit();
        log(intCount, start, "int");



        System.out.println(String.format("End of test, mem free: %d/%d",
                Runtime.getRuntime().freeMemory(), Runtime.getRuntime().totalMemory()));

        System.out.println(String.format("Starting flush/close at %s", new Date()));
        logger.close();
        System.out.println(String.format("Finished flush/close at %s", new Date()));

        System.out.println(String.format("Logger closed, mem free: %d/%d",
                Runtime.getRuntime().freeMemory(), Runtime.getRuntime().totalMemory()));
        if(!new File(FILENAME + "-1").delete())
        {
            System.out.println("FAILED to delete file");
        }
        logger = null;
        System.gc();
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5L));

        System.out.println(String.format("Deleted file at %s", new Date()));
        logger = new DirectMemoryLogger(FILENAME + "-2", fileSize);
        loggingService = new LoggingService(logger);
        loggingEvent = loggingService.singleThreadedAccess().begin(TestLogCategory.ONE);

        start = System.nanoTime();
        for(int i = 0; i < longCount; i++)
        {
            if(i % 100000 == 0)
            {
                System.out.println(i);
            }
            loggingEvent.appendLong(i);
        }
        loggingEvent.commit();
        log(longCount, start, "long");

        start = System.nanoTime();
        for(int i = 0; i < intCount; i++)
        {
            loggingEvent.appendInt(i);
        }
        loggingEvent.commit();
        log(intCount, start, "int");


        logger.close();
    }

    @Test
    public void shouldUseSingleThreadedLoggingEvent() throws Exception
    {
        final LoggingCapacityAllocator capacityAllocator = new SingleThreadedLoggingCapacityAllocator();
        final LoggingEvent event = new SingleThreadedLoggingEvent(logger, capacityAllocator);

        assertLoggingEventBehaviour(event);
    }

    @Test
    public void shouldUseMultiThreadedLoggingEvent() throws Exception
    {
        final LoggingCapacityAllocator capacityAllocator = new MultiThreadedLoggingCapacityAllocator();
        final LoggingEvent event = new MultiThreadedLoggingEvent(logger, capacityAllocator);

        assertLoggingEventBehaviour(event);
    }

    @Test
    public void shouldEncodePrimitives() throws Exception
    {
        logger.writeAsciiCharAt(0L, ASCII_CHAR).writeByteAt(1L, BYTE).
                writeIntAt(2L, INT).writeLongAt(6L, LONG).
                writeFloatAt(14L, FLOAT).writeDoubleAt(18L, DOUBLE).
                close();

        final RandomAccessFile file = new RandomAccessFile(FILENAME, "r");
        final byte[] input = getFileContent(file);

        assertThat(unsafe.getByte(input, byteArrayOffset), is((byte) ASCII_CHAR));
        assertThat(unsafe.getByte(input, 1 + byteArrayOffset), is(BYTE));
        assertThat(unsafe.getInt(input, 2 + byteArrayOffset), is(INT));
        assertThat(unsafe.getLong(input, 6 + byteArrayOffset), is(LONG));
        assertThat(unsafe.getFloat(input, 14 + byteArrayOffset), is(FLOAT));
        assertThat(unsafe.getDouble(input, 18 + byteArrayOffset), is(DOUBLE));

        file.close();
    }

    @Test
    public void shouldWriteByteArray() throws Exception
    {
        logger.writeByteArrayAt(0L, new byte[]{1, 2, 3, 4, 15, 16, 17, 18}, 8);

        final RandomAccessFile file = new RandomAccessFile(FILENAME, "r");
        final byte[] input = getFileContent(file);

        assertThat(unsafe.getByte(input, 0 + byteArrayOffset), is((byte) 1));
        assertThat(unsafe.getByte(input, 1 + byteArrayOffset), is((byte) 2));
        assertThat(unsafe.getByte(input, 2 + byteArrayOffset), is((byte) 3));
        assertThat(unsafe.getByte(input, 3 + byteArrayOffset), is((byte) 4));
        assertThat(unsafe.getByte(input, 4 + byteArrayOffset), is((byte) 15));
        assertThat(unsafe.getByte(input, 5 + byteArrayOffset), is((byte) 16));
        assertThat(unsafe.getByte(input, 6 + byteArrayOffset), is((byte) 17));
        assertThat(unsafe.getByte(input, 7 + byteArrayOffset), is((byte) 18));

        file.close();
    }

    @Test
    public void shouldDeserialiseSingleRecord() throws Exception
    {
        final LoggingCapacityAllocator capacityAllocator = new SingleThreadedLoggingCapacityAllocator();
        final LoggingEvent event = new SingleThreadedLoggingEvent(logger, capacityAllocator);

        final long longValue = System.currentTimeMillis();
        final int intValue = 1234;
        final char charOne = 'm';
        final char charTwo = 'p';
        final String expectedOutput = String.format(TestLogCategory.CATEGORY_ONE_FORMAT,
                new Date(longValue), intValue, charOne, charTwo);

        event.begin(TestLogCategory.ONE).appendLong(longValue).
                appendInt(intValue).appendAsciiChar(charOne).
                appendAsciiChar(charTwo).commit();

        logger.flush();

        final RandomAccessFile file = new RandomAccessFile(FILENAME, "r");
        final byte[] input = getFileContent(file);

        final CapturingLogRecordHandler recordHandler = new CapturingLogRecordHandler();
        getLogReplayer(input).replay(recordHandler);

        assertThat(recordHandler.getCapturedRecords().size(), is(1));
        assertThat(recordHandler.getCapturedRecords().get(0), is(equalTo(expectedOutput)));

        file.close();
    }

    @Test
    public void shouldDeserialiseMultipleRecords() throws Exception
    {
        final LoggingCapacityAllocator capacityAllocator = new SingleThreadedLoggingCapacityAllocator();
        final LoggingEvent event = new SingleThreadedLoggingEvent(logger, capacityAllocator);

        event.begin(TestLogCategory.ONE).appendLong(1234567890123L).
                appendInt(1234).appendAsciiChar('m').
                appendAsciiChar('p').commit();
        event.begin(TestLogCategory.ONE).appendLong(1234569890123L).
                appendInt(5678).appendAsciiChar('t').
                appendAsciiChar('g').commit();
        event.begin(TestLogCategory.TWO).appendLong(1234569890123L).
                appendFloat(123.456f).appendFloat(456.789f).
                appendFloat(987.654f).commit();
        event.begin(TestLogCategory.ONE).appendLong(1234569890123L).
                appendInt(9999).appendAsciiChar('a').
                appendAsciiChar('b').commit();
        event.begin(TestLogCategory.TWO).appendLong(1234569890123L).
                appendFloat(444.555f).appendFloat(666.777f).
                appendFloat(123.987f).commit();

        logger.flush();

        final RandomAccessFile file = new RandomAccessFile(FILENAME, "r");
        final byte[] input = getFileContent(file);

        final CapturingLogRecordHandler recordHandler = new CapturingLogRecordHandler();
        getLogReplayer(input).replay(recordHandler);

        final List<String> records = recordHandler.getCapturedRecords();
        assertThat(records.size(), is(5));
        assertThat(records.get(0), is(equalTo("Timestamp [Fri Feb 13 23:31:30 GMT 2009], count: 1234, chars: m, p")));
        assertThat(records.get(1), is(equalTo("Timestamp [Sat Feb 14 00:04:50 GMT 2009], count: 5678, chars: t, g")));
        assertThat(records.get(2), is(equalTo("Timestamp [Sat Feb 14 00:04:50 GMT 2009], a: 123.456, b: 456.789, c: 987.654")));
        assertThat(records.get(3), is(equalTo("Timestamp [Sat Feb 14 00:04:50 GMT 2009], count: 9999, chars: a, b")));
        assertThat(records.get(4), is(equalTo("Timestamp [Sat Feb 14 00:04:50 GMT 2009], a: 444.555, b: 666.777, c: 123.987")));

        file.close();
    }

    private void log(final int iterations, final long start, final String type)
    {
        final long duration = System.nanoTime() - start;
        System.out.println(String.format("%s: %d writes in %dms (%.2f/ms, avg %.2fns)", type, iterations,
                TimeUnit.NANOSECONDS.toMillis(duration),
                (iterations / (float) TimeUnit.NANOSECONDS.toMillis(duration)),
                duration / (float) iterations));
    }

    private LogReplay getLogReplayer(final byte[] input)
    {
        return new LogReplay(new BinaryLogRecordHandlerRegistry().initialise(TestLogCategory.values()), input);
    }

    private static final class CapturingLogRecordHandler implements LogRecordHandler
    {
        private final List<String> capturedRecords = new ArrayList<String>();

        @Override
        public void onLogRecord(final String humanReadableLogRecord)
        {
            capturedRecords.add(humanReadableLogRecord);
        }

        public List<String> getCapturedRecords()
        {
            return capturedRecords;
        }
    }

    private void assertLoggingEventBehaviour(final LoggingEvent event) throws IOException
    {
        event.begin(TestLogCategory.ONE).
                appendAsciiChar(ASCII_CHAR).appendByte(BYTE).
                appendInt(INT).appendLong(LONG).
                appendFloat(FLOAT).appendDouble(DOUBLE).
                commit();


        final RandomAccessFile file = new RandomAccessFile(FILENAME, "r");
        final byte[] input = getFileContent(file);

        assertThat(unsafe.getInt(input, byteArrayOffset), is(TestLogCategory.ONE.getCategoryId()));
        assertThat(unsafe.getInt(input, 4 + byteArrayOffset), is(26));
        assertThat(unsafe.getByte(input, 8 + byteArrayOffset), is((byte) ASCII_CHAR));
        assertThat(unsafe.getByte(input, 9 + byteArrayOffset), is(BYTE));
        assertThat(unsafe.getInt(input, 10 + byteArrayOffset), is(INT));
        assertThat(unsafe.getLong(input, 14 + byteArrayOffset), is(LONG));
        assertThat(unsafe.getFloat(input, 22 + byteArrayOffset), is(FLOAT));
        assertThat(unsafe.getDouble(input, 26 + byteArrayOffset), is(DOUBLE));

        file.close();
    }

    private byte[] getFileContent(final RandomAccessFile file) throws IOException
    {
        final byte[] input = new byte[ONE_KILOBYTE];
        if(file.read(input) != ONE_KILOBYTE)
        {
            file.close();
            fail("Unable to read expected number of bytes from file");
        }
        return input;
    }
}