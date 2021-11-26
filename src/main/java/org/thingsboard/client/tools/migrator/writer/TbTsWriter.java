package org.thingsboard.client.tools.migrator.writer;

import com.google.common.collect.Lists;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.thingsboard.client.tools.migrator.DictionaryParser;
import org.thingsboard.client.tools.migrator.RelatedEntitiesParser;
import org.thingsboard.client.tools.migrator.WriterBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class TbTsWriter extends AbstractTbWriter {

    private final CQLSSTableWriter currentPartitionsWriter;

    public TbTsWriter(DictionaryParser keyParser, RelatedEntitiesParser entityIdsAndTypes, File outDir,
                      File outTsPartitionDir, boolean castStringsIfPossible, String partitioning) {
        super(keyParser, entityIdsAndTypes, outDir, castStringsIfPossible, partitioning);
        this.currentPartitionsWriter = WriterBuilder.getPartitionWriter(outTsPartitionDir);
    }

    @Override
    public void closeWriters() throws IOException {
        super.closeWriters();
        if (currentPartitionsWriter != null) {
            currentPartitionsWriter.close();
        }
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
        currentWriter = WriterBuilder.getTsWriter(outDir);
    }

    @Override
    public CQLSSTableWriter getWriter(File outDir) {
        return WriterBuilder.getTsWriter(outDir);
    }

    @Override
    public void writePartitions() throws IOException {
        System.out.println("Partitions collected " + partitions.size());
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

        System.out.println(new Date() + " Migrated partitions " + partitions.size() + " in " + (System.currentTimeMillis() - startTs));
    }
}
