package com.epickrram.binlog;//////////////////////////////////////////////////////////////////////////////////
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

import java.lang.reflect.Field;

public final class UnsafeUtil
{
    public static long getByteArrayBaseOffset()
    {
        return getUnsafe().arrayBaseOffset(byte[].class);
    }

    public static Unsafe getUnsafe()
    {
        try
        {
            final Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        }
        catch (NoSuchFieldException e)
        {
            throw new IllegalStateException("Unable to access Unsafe", e);
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalStateException("Unable to access Unsafe", e);
        }
    }
}
