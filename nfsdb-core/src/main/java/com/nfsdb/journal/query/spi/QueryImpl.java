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

package com.nfsdb.journal.query.spi;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.query.api.Query;
import com.nfsdb.journal.query.api.QueryAll;
import com.nfsdb.journal.query.api.QueryHead;

public class QueryImpl<T> implements Query<T> {

    private final Journal<T> journal;
    private final QueryAllImpl<T> allImpl;
    private final QueryHeadImpl<T> headImpl;

    public QueryImpl(Journal<T> journal) {
        this.journal = journal;
        this.allImpl = new QueryAllImpl<>(journal);
        this.headImpl = new QueryHeadImpl<>(journal);
    }

    @Override
    public QueryAll<T> all() {
        return allImpl;
    }

    @Override
    public QueryHead<T> head() {
        return headImpl;
    }

    @Override
    public Journal<T> getJournal() {
        return journal;
    }
}
