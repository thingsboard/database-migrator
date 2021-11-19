package org.thingsboard.client.tools.migrator.writer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.math.NumberUtils;
import org.thingsboard.client.tools.migrator.DictionaryParser;
import org.thingsboard.client.tools.migrator.RelatedEntitiesParser;
import org.thingsboard.client.tools.migrator.WriterUtils;
import org.thingsboard.client.tools.migrator.exception.EntityMissingException;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractTbWriter implements TbWriter {

    private static final long LOG_BATCH = 1000000;
    private static final long ROW_PER_FILE = 1000000;

    private long castErrors = 0;
    private long castedOk = 0;

    protected long currentWriterCount = 1;

    protected final Set<String> partitions = new HashSet<>();

    protected long linesMigrated = 0;

    private long skippedLines = 0;

    private final DictionaryParser keyParser;
    private final RelatedEntitiesParser entityIdsAndTypes;
    protected CQLSSTableWriter currentWriter;
    protected final File outDir;

    private final boolean castStringIfPossible;

    public AbstractTbWriter(DictionaryParser keyParser, RelatedEntitiesParser entityIdsAndTypes, File outDir, boolean castStringIfPossible) {
        this.keyParser = keyParser;
        this.entityIdsAndTypes = entityIdsAndTypes;
        this.currentWriter = getWriter(outDir);
        this.outDir = outDir;
        this.castStringIfPossible = castStringIfPossible;
    }

    public abstract List<Object> toValues(List<String> raw);

    public abstract void reOpenWriter() throws IOException;

    public abstract CQLSSTableWriter getWriter(File outDir);

    @Override
    public long getLinesMigrated() {
        return linesMigrated;
    }

    @Override
    public long getSkippedLines() {
        return skippedLines;
    }

    @Override
    public void closeWriters() throws IOException {
        if (currentWriter != null) {
            currentWriter.close();
        }
    }

    @Override
    public void processBlock(LineIterator iterator) {
        String currentLine;
        long linesProcessed = 0;
        while (iterator.hasNext()) {
            logLinesProcessed(linesProcessed++);
            currentLine = iterator.nextLine();

            if (WriterUtils.isBlockFinished(currentLine)) {
                return;
            }

            List<Object> values = null;
            try {
                List<String> raw = Arrays.stream(currentLine.trim().split("\t"))
                        .map(String::trim)
                        .collect(Collectors.toList());
                try {
                    values = toValues(raw);
                } catch (EntityMissingException e) {
                    log.debug("Failed to process line [{}}, cause {}, skipping it ", currentLine, e.getMessage());
                }

                if (values != null) {
                    if (this.currentWriterCount == 0) {
                        System.out.println(new Date() + " close writer " + new Date());
                        reOpenWriter();

                    }

                    if (this.castStringIfPossible) {
                        currentWriter.addRow(castToNumericIfPossible(values));
                    } else {
                        currentWriter.addRow(values);
                    }

                    currentWriterCount++;
                    if (currentWriterCount >= ROW_PER_FILE) {
                        currentWriterCount = 0;
                    }
                } else {
                    skippedLines++;
                }
            } catch (Exception ex) {
                String strValues = "";
                if (values != null) {
                    strValues = values.toString();
                }
                log.error("Failed to process line [" + currentLine + "], skipping it , values = " + strValues + "", ex);
            }
        }
    }

    private void logLinesProcessed(long lines) {
        if (lines > 0 && lines % LOG_BATCH == 0) {
            log.info("Lines processed {}, castOk {}, castErr {}, skippedLines {}",
                    lines, castedOk, castErrors, skippedLines);
        }
    }


    private List<Object> castToNumericIfPossible(List<Object> values) {
        try {
            if (values.get(6) != null && NumberUtils.isNumber(values.get(6).toString())) {
                Double casted = NumberUtils.createDouble(values.get(6).toString());
                List<Object> numeric = Lists.newArrayList();
                numeric.addAll(values);
                numeric.set(6, null);
                numeric.set(8, casted);
                castedOk++;
                return numeric;
            }
        } catch (Throwable th) {
            castErrors++;
        }

        processPartitions(values);

        return values;
    }

    protected void processPartitions(List<Object> values) {
        String key = values.get(0) + "|" + values.get(1) + "|" + values.get(2) + "|" + values.get(3);
        partitions.add(key);
    }

    protected void logLinesMigrated(long lines) {
        if (lines > 0 && lines % LOG_BATCH == 0) {
            log.info("Lines migrated {}, castOk {}, castErr {}, skippedLines {}",
                    lines, castedOk, castErrors, skippedLines);
        }
    }

    protected void addTypeIdKey(List<Object> result, List<String> raw) {
        String entityType = entityIdsAndTypes.getEntityType(raw.get(0));
        if (entityType == null) {
            String errorMsg = String.format("Can't find entity type for ID [%s], most probably entity was removed from the system",
                    raw.get(0));
            throw new EntityMissingException(errorMsg);
        }
        result.add(entityType);
        result.add(UUID.fromString(raw.get(0)));
        result.add(keyParser.getKeyByKeyId(raw.get(1)));
    }

    protected void addPartitions(List<Object> result, List<String> raw) {
        long ts = Long.parseLong(raw.get(2));
        long partition = toPartitionTs(ts);
        result.add(partition);
        result.add(ts);
    }

    protected void addTimeseries(List<Object> result, List<String> raw) {
        result.add(Long.parseLong(raw.get(2)));
    }

    protected void addValues(List<Object> result, List<String> raw) {
        result.add(raw.get(3).equals("\\N") ? null : raw.get(3).equals("t") ? Boolean.TRUE : Boolean.FALSE);
        result.add(raw.get(4).equals("\\N") ? null : raw.get(4));
        result.add(raw.get(5).equals("\\N") ? null : Long.parseLong(raw.get(5)));
        result.add(raw.get(6).equals("\\N") ? null : Double.parseDouble(raw.get(6)));
        result.add(raw.get(7).equals("\\N") ? null : raw.get(7));
    }

    private long toPartitionTs(long ts) {
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneOffset.UTC);
        return time.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

}
