/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.std;

public class BitmapIndexUtilsNative {
    public static native void empty();

    public static native void latestScanBackward(long keysMemory, long keysMemorySize, long valuesMemory,
                                                 long valuesMemorySize, long rowsMemory, long rowsMemorySize,
                                                 long keyBegin, long keyEnd, long maxValue, long minValue,
                                                 int partitionIndex, int blockValueCountMod);

    private static native void seekValueBlockRTL0(long valueCount, long blockOffset, long valueMem,
                                                 long maxValue, long blockValueCountMod);

    private static native void seekValueBlockLTR0(long initialCount, long firstValueBlockOffset, long valueMem,
                                                  long minValue, long blockValueCountMod);

    public static void seekValueBlockRTL(long valueCount, long blockOffset, long valueMem,
                                  long maxValue, long blockValueCountMod) {
        seekValueBlockRTL0(valueCount, blockOffset, valueMem, maxValue, blockValueCountMod);
    }

    public static void seekValueBlockLTR(long initialCount, long firstValueBlockOffset, long valueMem,
                                         long minValue, long blockValueCountMod) {
        seekValueBlockLTR0(initialCount, firstValueBlockOffset, valueMem, minValue, blockValueCountMod);
    }
}
