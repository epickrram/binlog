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


package com.epickrram.binlog;

import org.junit.Before;
import org.junit.Test;
import sun.misc.Unsafe;

import java.io.File;

import static com.epickrram.binlog.DirectMemoryLogger.SIZE_OF_INT;

public final class SingleThreadedFileWrappingIntegrationTest
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
    private LoggingEvent loggingEvent;

    @Before
    public void setup() throws Exception
    {
        new File(FILENAME).delete();
        logger = new DirectMemoryLogger(FILENAME, ONE_KILOBYTE);
        unsafe = UnsafeUtil.getUnsafe();
        loggingEvent = new LoggingService(logger).singleThreadedAccess();
    }

    @Test
    public void shouldRollToNewFileWhenCurrentFileIsFull() throws Exception
    {
        final int intCount = ONE_KILOBYTE / (SIZE_OF_INT * 3);
        for(int i = 0; i < intCount; i++)
        {
            loggingEvent.begin(TestLogCategory.THREE).appendInt(i).commit();
        }

        loggingEvent.appendInt(intCount).commit();


    }
}