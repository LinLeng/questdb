/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.utils;

public final class Rows {

    public static long toRowID(int partitionIndex, long localRowID) {
        return (((long) partitionIndex) << 44L) + localRowID;
    }

    public static int toPartitionIndex(long rowID) {
        return (int) (rowID >>> 44);
    }

    public static long toLocalRowID(long rowID) {
        return rowID & 0xFFFFFFFFFFFL;
    }

    private Rows() {
    }
}
