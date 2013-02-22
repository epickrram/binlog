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


import java.util.concurrent.atomic.AtomicLong;

public final class MultiThreadedLoggingCapacityAllocator implements LoggingCapacityAllocator
{
    private final AtomicLong position = new AtomicLong(0L);

    @Override
    public long getAllocatedBlockAddress(final long requiredCapacity)
    {
        while(true)
        {
            final long current = position.get();
            final long target = current + requiredCapacity;
            if(position.compareAndSet(current, target))
            {
                return current;
            }
        }
    }

    @Override
    public void onNewFile()
    {
        reset();
    }

    @Override
    public void reset()
    {
        position.set(0L);
    }
}
