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
        super(keyParser, entityIdsAndTypes, outDir, castStringsIfPossible, partitioning);
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
