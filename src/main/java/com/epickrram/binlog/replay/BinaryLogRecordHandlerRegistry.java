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


import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

public final class BinaryLogRecordHandlerRegistry
{
    private final Map<Integer, BinaryLogRecordHandler> logCategoryMap = new HashMap<Integer, BinaryLogRecordHandler>();

    public BinaryLogRecordHandler getByCategoryId(final int logCategoryId)
    {
        if(!logCategoryMap.containsKey(logCategoryId))
        {
            throw new IllegalArgumentException(format("Unknown category: %d, was registry property initialised?", logCategoryId));
        }
        return logCategoryMap.get(logCategoryId);
    }

    public BinaryLogRecordHandlerRegistry initialise(final Collection<BinaryLogRecordHandler> recordHandlers)
    {
        for (BinaryLogRecordHandler recordHandler : recordHandlers)
        {
            logCategoryMap.put(recordHandler.getCategoryId(), recordHandler);
        }

        return this;
    }

    public BinaryLogRecordHandlerRegistry initialise(final BinaryLogRecordHandler[] recordHandlers)
    {
        for (BinaryLogRecordHandler recordHandler : recordHandlers)
        {
            logCategoryMap.put(recordHandler.getCategoryId(), recordHandler);
        }

        return this;
    }
}
