package org.thingsboard.client.tools.migrator.writer;

import org.apache.commons.io.LineIterator;

import java.io.IOException;

public interface TbWriter {

    int processBlock(LineIterator iterator, int linesToSkip);

    void writePartitions() throws IOException;

    void closeWriters() throws IOException;

    long getLinesMigrated();

    long getSkippedLines();
}
