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

#include "bitmap_index_utils.h"
#include <algorithm>

void latest_scan_backward(args &a) {
    auto keys_memory = reinterpret_cast<const uint8_t *>(a.keys_memory);
    auto values_memory = reinterpret_cast<const uint8_t *>(a.values_memory);
    auto rows = reinterpret_cast<int64_t *>(a.rows_memory);
    auto rows_length = a.rows_memory_size / sizeof(int64_t);
    auto header  = reinterpret_cast<const key_header *>(keys_memory);
    auto keys  = reinterpret_cast<const key_entry *>(keys_memory + sizeof(key_header));
    auto key_count = (int64_t)header->key_count; //FIXME: key_count from java part
    auto key_begin = a.key_begin;
    auto key_end = std::min(a.key_end, key_count);
    auto min_value = a.min_value;
    auto max_value = a.max_value;
    auto partition_index = a.partition_index;
    auto vblock_capacity_mask = a.block_value_count_mod;
    auto value_memory_size = a.values_memory_size;
    const size_t vblock_capacity = vblock_capacity_mask + 1;

    int64_t local_key_begin = std::numeric_limits<int64_t>::max();
    int64_t local_key_end = std::numeric_limits<int64_t>::min();
    const size_t vblock_size = vblock_capacity * sizeof(int64_t) + sizeof(value_block_link);

    for(int64_t k = key_begin; k < key_end; ++k) {

        if(rows[k + 2] > 0) continue; // skip if found

        uint64_t value_count = 0;
        uint64_t last_vblock_offset = 0;
        uint64_t first_vblock_offset = 0;
        uint64_t retries_count = 10;
        bool is_kblock_consistent = false;
        while(retries_count--) {
            value_count = keys[k].value_count;
            std::atomic_thread_fence(std::memory_order_acquire);
            if (keys[k].count_check == value_count) {
                first_vblock_offset = keys[k].first_value_block_offset;
                last_vblock_offset = keys[k].last_value_block_offset;
                std::atomic_thread_fence(std::memory_order_acquire);
                if (keys[k].value_count == value_count) {
                    is_kblock_consistent = true;
                    break;
                }
            }
        }
        bool update_range = true;
        if(value_count > 0) {
            bool is_offset_in_mapped_area = last_vblock_offset + vblock_size <= value_memory_size;

            if (!is_kblock_consistent || !is_offset_in_mapped_area) {
                // can trust only first vblock offset
                last_vblock_offset = first_vblock_offset;
                int64_t vblock_end_offset = last_vblock_offset + vblock_size;
                auto link = reinterpret_cast<const value_block_link *>(vblock_end_offset - sizeof(value_block_link));
                uint64_t block_traversed = 1;
                while(link->next && link->next + vblock_size < value_memory_size) {
                    last_vblock_offset = link->next;
                    block_traversed += 1;
                }
                //assuming blocks are full
                value_count = vblock_capacity*block_traversed;
            }

            auto values = reinterpret_cast<const int64_t *>(values_memory);
            auto res = seekValueBlockRTL(value_count, last_vblock_offset, values, max_value,
                                         vblock_capacity_mask);
            value_count = res.first;
            last_vblock_offset = res.second;
            if (value_count > 0) {
                uint64_t cell_index = --value_count & vblock_capacity_mask;
                int64_t local_row_id = values[last_vblock_offset / 8 + cell_index];
                if (local_row_id >= min_value) {
                    rows[k + 2] = ((int64_t) partition_index << 44) + local_row_id;
                    update_range = false;
                }
            }
        }

        if (update_range) {
            if (k < local_key_begin) local_key_begin = k;
            if (k > local_key_end) local_key_end = k;
        }
    }
    rows[0] = local_key_begin;
    rows[1] = local_key_end;
}

extern "C" {

JNIEXPORT void JNICALL
Java_io_questdb_std_BitmapIndexUtilsNative_seekValueBlockRTL0(JNIEnv *env, jclass cl, jlong valueCount,
                                                              jlong blockOffset, jlong valueMem, jlong maxValue,
                                                              jlong blockValueCountMod) {

    auto value_count = static_cast<int64_t>(valueCount);
    auto block_offset = static_cast<int64_t>(blockOffset);
    auto *memory = reinterpret_cast<const int64_t *>(valueMem);
    auto max_value = static_cast<int64_t>(maxValue);
    auto block_value_count_mod = static_cast<int64_t>(blockValueCountMod);

    std::pair<int64_t, int64_t> res;
    res = seekValueBlockRTL(value_count, block_offset, memory, max_value, block_value_count_mod);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_BitmapIndexUtilsNative_seekValueBlockLTR0(JNIEnv *env, jclass cl, jlong initialCount,
                                                              jlong firstValueBlockOffset, jlong valueMem,
                                                              jlong minValue,
                                                              jlong blockValueCountMod) {

    auto value_count = static_cast<int64_t>(initialCount);
    auto block_offset = static_cast<int64_t>(firstValueBlockOffset);
    auto *memory = reinterpret_cast<const int64_t *>(valueMem);
    auto min_value = static_cast<int64_t>(minValue);
    auto block_value_count_mod = static_cast<int64_t>(blockValueCountMod);

    std::pair<int64_t, int64_t> res;
    res = seekValueBlockLTR(value_count, block_offset, memory, min_value, block_value_count_mod);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_BitmapIndexUtilsNative_latestScanBackward(JNIEnv *env, jclass cl
                                                , jlong keysMemory
                                                , jlong keysMemorySize
                                                , jlong valuesMemory
                                                , jlong valuesMemorySize
                                                , jlong rowsMemory
                                                , jlong rowsMemorySize
                                                , jlong keyBegin
                                                , jlong keyEnd
                                                , jlong maxValue
                                                , jlong minValue
                                                , jint partitionIndex
                                                , jint blockValueCountMod) {

    args a = {
            static_cast<uint64_t>(keysMemory),
            static_cast<uint64_t>(keysMemorySize),
            static_cast<uint64_t>(valuesMemory),
            static_cast<uint64_t>(valuesMemorySize),
            static_cast<uint64_t>(rowsMemory),
            static_cast<uint64_t>(rowsMemorySize),
            keyBegin,
            keyEnd,
            maxValue,
            minValue,
            partitionIndex,
            blockValueCountMod
    };
    latest_scan_backward(a);
}

} // extern "C"