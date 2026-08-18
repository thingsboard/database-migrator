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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.thingsboard.client.tools.migrator.DictionaryParser;
import org.thingsboard.client.tools.migrator.RelatedEntitiesParser;
import org.thingsboard.client.tools.migrator.WriterBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
public class TbTsWriter extends AbstractTbWriter {

    private final File outTsPartitionDir;

    public TbTsWriter(DictionaryParser keyParser, RelatedEntitiesParser entityIdsAndTypes, File outDir,
                      File outTsPartitionDir, boolean castStringsIfPossible, String partitioning, Long ttlSeconds) {
        super(keyParser, entityIdsAndTypes, outDir, castStringsIfPossible, partitioning, ttlSeconds);
        this.outTsPartitionDir = outTsPartitionDir;
    }

    @Override
    public void closeWriters() throws IOException {
        super.closeWriters();
    }

    @Override
    public List<Object> toValues(List<String> raw) {
        List<Object> result = new ArrayList<>();

        addTypeIdKey(result, raw);
        addPartitions(result, raw);
        addValues(result, raw);
        processPartitions(result);

        logLinesMigrated(linesMigrated++);

        return result;
    }

    @Override
    public void reOpenWriter() throws IOException {
        currentWriter.close();
        currentWriter = WriterBuilder.getTsWriter(outDir, ttlSeconds);
    }

    @Override
    public CQLSSTableWriter getWriter(File outDir) {
        return WriterBuilder.getTsWriter(outDir, ttlSeconds);
    }

    @Override
    public void writePartitions() throws IOException {
        CQLSSTableWriter currentPartitionsWriter = null;
        try {
            currentPartitionsWriter = WriterBuilder.getPartitionWriter(outTsPartitionDir, ttlSeconds);
            log.info("Partitions collected " + partitions.size());
            long startTs = System.currentTimeMillis();
            for (String partition : partitions) {
                String[] split = partition.split("\\|");
                List<Object> values = Lists.newArrayList();
                values.add(split[0]);
                values.add(UUID.fromString(split[1]));
                values.add(split[2]);
                values.add(Long.parseLong(split[3]));
                currentPartitionsWriter.addRow(values);
            }

            log.info(" Migrated partitions " + partitions.size() + " in " + (System.currentTimeMillis() - startTs));

            partitions.clear();
        } finally {
            if (currentPartitionsWriter != null) {
                currentPartitionsWriter.close();
            }
        }
    }
}
