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


package com.epickrram.store.onheap;

import com.epickrram.store.ByteReader;
import com.epickrram.store.ByteReaderHandler;
import com.epickrram.store.ByteWriter;
import com.epickrram.store.ByteWriterHandler;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public final class ByteBufferStoreTest
{
    private static final byte[] MESSAGE_ONE = new byte[] {9, 8, 7, 6, 5, 4, 3, 2};
    private static final byte[] MESSAGE_TWO = new byte[60];

    static
    {
        Arrays.fill(MESSAGE_TWO, (byte)7);
    }

    private ByteBufferStore byteBufferStore;

    @Before
    public void setup()
    {
        byteBufferStore = new ByteBufferStore(64, 8);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailIfSequenceDoesNotExist() throws Exception
    {
        byteBufferStore.get(17L, new ByteArrayByteReaderHandler(8));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailIfSequenceHasBeenOverwritten() throws Exception
    {
        for(int i = 0; i < 9; i++)
        {
            byteBufferStore.put(i, new ByteArrayByteWriterHandler(MESSAGE_ONE));
        }

        byteBufferStore.get(0L, new ByteArrayByteReaderHandler(8));
    }

    @Test
    public void shouldGetAndPutRecordsThatWrapStore() throws Exception
    {
        byteBufferStore.put(0L, new ByteArrayByteWriterHandler(MESSAGE_TWO));
        byteBufferStore.put(1L, new ByteArrayByteWriterHandler(MESSAGE_ONE));

        final ByteArrayByteReaderHandler byteReaderHandler = new ByteArrayByteReaderHandler(MESSAGE_ONE.length);
        byteBufferStore.get(1L, byteReaderHandler);

        assertEquality(MESSAGE_ONE, byteReaderHandler.message);
    }

    @Test
    public void shouldPutAndGetOneRecord() throws Exception
    {
        byteBufferStore.put(0L, new ByteArrayByteWriterHandler(MESSAGE_ONE));
        final ByteArrayByteReaderHandler byteReaderHandler = new ByteArrayByteReaderHandler(MESSAGE_ONE.length);
        byteBufferStore.get(0L, byteReaderHandler);

        assertEquality(MESSAGE_ONE, byteReaderHandler.message);
    }

    private void assertEquality(final byte[] expected, final byte[] actual)
    {
        assertThat(String.format("Expected:%n%s%nFound:%n%s%n", Arrays.toString(expected), Arrays.toString(actual)),
                Arrays.equals(expected, actual), is(true));
    }

    private static final class ByteArrayByteReaderHandler implements ByteReaderHandler
    {
        private final byte[] message;

        public ByteArrayByteReaderHandler(final int size)
        {
            message = new byte[size];
        }

        @Override
        public void read(final ByteReader byteReader)
        {
            byteReader.read(message, message.length);
        }
    }

    private static final class ByteArrayByteWriterHandler implements ByteWriterHandler
    {
        private final byte[] message;

        public ByteArrayByteWriterHandler(final byte[] message)
        {
            this.message = message;
        }

        @Override
        public void write(final ByteWriter byteWriter, final long maxLength)
        {
            byteWriter.write(message, 0L);
        }
    }
}
