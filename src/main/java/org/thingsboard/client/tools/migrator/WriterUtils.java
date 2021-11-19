package org.thingsboard.client.tools.migrator;

import org.apache.commons.lang3.StringUtils;

public class WriterUtils {

    private WriterUtils() {}

    public static boolean isBlockFinished(String line) {
        return StringUtils.isBlank(line) || line.equals("\\.");
    }
}
