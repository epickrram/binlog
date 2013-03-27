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

import java.io.File;

public final class SequentialRandomAccessFileBufferAllocatorTest
{
    private static final String BASENAME = "srafb-test";
    private SequentialRandomAccessFileBufferAllocator allocator;

    @Before
    public void before() throws Exception
    {
        allocator = new SequentialRandomAccessFileBufferAllocator(BASENAME, createTestDirectory());
    }

    @Test
    public void shouldReturnDifferentBuffersOnSequentialCalls() throws Exception
    {

    }

    private static File createTestDirectory()
    {
        final File dataDir = new File(System.getProperty("java.io.tmpdir") + File.separator +
                System.getProperty("user.name") + File.separator + BASENAME);
        dataDir.mkdirs();
        return dataDir;
    }
}