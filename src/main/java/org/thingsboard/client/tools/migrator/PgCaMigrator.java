/**
 * ThingsBoard, Inc. ("COMPANY") CONFIDENTIAL
 *
 * Copyright © 2016-2021 ThingsBoard, Inc. All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains
 * the property of ThingsBoard, Inc. and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to ThingsBoard, Inc.
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 *
 * Dissemination of this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from COMPANY.
 *
 * Access to the source code contained herein is hereby forbidden to anyone except current COMPANY employees,
 * managers or contractors who have executed Confidentiality and Non-disclosure agreements
 * explicitly covering such access.
 *
 * The copyright notice above does not evidence any actual or intended publication
 * or disclosure  of  this source code, which includes
 * information that is confidential and/or proprietary, and is a trade secret, of  COMPANY.
 * ANY REPRODUCTION, MODIFICATION, DISTRIBUTION, PUBLIC  PERFORMANCE,
 * OR PUBLIC DISPLAY OF OR THROUGH USE  OF THIS  SOURCE CODE  WITHOUT
 * THE EXPRESS WRITTEN CONSENT OF COMPANY IS STRICTLY PROHIBITED,
 * AND IN VIOLATION OF APPLICABLE LAWS AND INTERNATIONAL TREATIES.
 * THE RECEIPT OR POSSESSION OF THIS SOURCE CODE AND/OR RELATED INFORMATION
 * DOES NOT CONVEY OR IMPLY ANY RIGHTS TO REPRODUCE, DISCLOSE OR DISTRIBUTE ITS CONTENTS,
 * OR TO MANUFACTURE, USE, OR SELL ANYTHING THAT IT  MAY DESCRIBE, IN WHOLE OR IN PART.
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
        return line.startsWith("COPY public.ts_kv (");
    }

    private boolean isBlockLatestStarted(String line) {
        return line.startsWith("COPY public.ts_kv_latest (");
    }
}
