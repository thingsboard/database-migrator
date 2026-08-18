/**
 * Copyright © 2016-2026 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.client.tools.migrator.writer;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.thingsboard.client.tools.migrator.DictionaryParser;
import org.thingsboard.client.tools.migrator.RelatedEntitiesParser;
import org.thingsboard.client.tools.migrator.WriterBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TbLatestWriter extends AbstractTbWriter {

    public TbLatestWriter(DictionaryParser keyParser, RelatedEntitiesParser entityIdsAndTypes, File outDir,
                          boolean castStringsIfPossible, String partitioning) {
        // latest values never expire regardless of the migration's ttl setting
        super(keyParser, entityIdsAndTypes, outDir, castStringsIfPossible, partitioning, null);
    }

    @Override
    public CQLSSTableWriter getWriter(File outDir) {
        return WriterBuilder.getLatestWriter(outDir);
    }

    @Override
    public List<Object> toValues(List<String> raw) {
        List<Object> result = new ArrayList<>();

        addTypeIdKey(result, raw);
        addTimeseries(result, raw);
        addValues(result, raw);

        logLinesMigrated(linesMigrated++);

        return result;
    }

    @Override
    public void reOpenWriter() throws IOException {
        currentWriter.close();
        currentWriter = WriterBuilder.getLatestWriter(outDir);
    }

    @Override
    public void writePartitions() {
        // nothing todo
    }

}
