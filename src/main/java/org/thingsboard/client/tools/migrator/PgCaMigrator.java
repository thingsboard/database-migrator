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
package org.thingsboard.client.tools.migrator;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.thingsboard.client.tools.migrator.writer.TbLatestWriter;
import org.thingsboard.client.tools.migrator.writer.TbTsWriter;
import org.thingsboard.client.tools.migrator.writer.TbWriter;

import java.io.File;
import java.io.IOException;

@Slf4j
public class PgCaMigrator {

    private final File sourceFile;
    private TbWriter tbLatestWriter;
    private TbWriter tbTsWriter;

    public PgCaMigrator(File sourceFile,
                        File ourTsDir,
                        File outTsPartitionDir,
                        File outTsLatestDir,
                        RelatedEntitiesParser allEntityIdsAndTypes,
                        DictionaryParser dictionaryParser,
                        boolean castStringsIfPossible,
                        String partitioning) {
        this.sourceFile = sourceFile;
        if (outTsLatestDir != null) {
            this.tbLatestWriter = new TbLatestWriter(dictionaryParser, allEntityIdsAndTypes, outTsLatestDir, castStringsIfPossible, partitioning);
        }
        if (ourTsDir != null) {
            this.tbTsWriter = new TbTsWriter(dictionaryParser, allEntityIdsAndTypes, ourTsDir, outTsPartitionDir, castStringsIfPossible, partitioning);
        }
    }

    public void migrate(Integer linesToSkip) throws IOException {
        String line;
        LineIterator iterator = FileUtils.lineIterator(this.sourceFile);

        try {
            while (iterator.hasNext()) {
                line = iterator.nextLine();
                if (this.tbLatestWriter != null && isBlockLatestStarted(line)) {
                    log.info("START TO MIGRATE LATEST");
                    long start = System.currentTimeMillis();
                    linesToSkip = tbLatestWriter.processBlock(iterator, linesToSkip);
                    log.info("TOTAL LINES MIGRATED: {}, FORMING OF SSL FOR LATEST TS FINISHED WITH TIME: {} ms, skipped lines {}",
                            tbLatestWriter.getLinesMigrated(), (System.currentTimeMillis() - start), tbLatestWriter.getSkippedLines());
                }

                if (this.tbTsWriter != null && isBlockTsStarted(line)) {
                    log.info("START TO MIGRATE TS");
                    long start = System.currentTimeMillis();
                    linesToSkip = tbTsWriter.processBlock(iterator, linesToSkip);
                    log.info("TOTAL LINES MIGRATED: {}, FORMING OF SSL FOR TS FINISHED WITH TIME: {} ms, skipped lines {}",
                            tbTsWriter.getLinesMigrated(), (System.currentTimeMillis() - start), tbTsWriter.getSkippedLines());
                }
            }

            log.info("Finished migrate Telemetry");

        } finally {
            iterator.close();
            if (this.tbTsWriter != null) {
                tbTsWriter.closeWriters();
            }
            if (this.tbLatestWriter != null) {
                tbLatestWriter.closeWriters();
            }
        }
    }

    private boolean isBlockTsStarted(String line) {
        return line.startsWith("COPY public.ts_kv (") || line.startsWith("COPY _timescaledb_internal._hyper") || line.startsWith("COPY public.ts_kv_custom");
    }

    private boolean isBlockLatestStarted(String line) {
        return line.startsWith("COPY public.ts_kv_latest (");
    }
}
