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
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;

@Slf4j
public class MigratorTool {

    public static void main(String[] args) {
        CommandLine cmd = parseArgs(args);

        try {
            boolean castEnable = Boolean.parseBoolean(cmd.getOptionValue("castEnable"));
            File allTelemetrySource = new File(cmd.getOptionValue("telemetryFrom"));
            File tsSaveDir = null;
            File partitionsSaveDir = null;
            File latestSaveDir = null;

            RelatedEntitiesParser allEntityIdsAndTypes =
                    new RelatedEntitiesParser(new File(cmd.getOptionValue("relatedEntities")));
            DictionaryParser dictionaryParser = new DictionaryParser(new File(cmd.getOptionValue("dictionary")));

            if (cmd.getOptionValue("latestTelemetryOut") != null) {
                latestSaveDir = new File(cmd.getOptionValue("latestTelemetryOut"));
            }
            if (cmd.getOptionValue("telemetryOut") != null) {
                tsSaveDir = new File(cmd.getOptionValue("telemetryOut"));
                partitionsSaveDir = new File(cmd.getOptionValue("partitionsOut"));
            }
            String partitioning = NoSqlTsPartitionDate.MONTHS.name();
            if (cmd.getOptionValue("partitioning") != null) {
                partitioning = cmd.getOptionValue("partitioning");
            }

            int linesToSkip = 0;
            if (cmd.getOptionValue("linesToSkip") != null) {
                linesToSkip = Integer.parseInt(cmd.getOptionValue("linesToSkip"));
            }

            new PgCaMigrator(
                    allTelemetrySource,
                    tsSaveDir,
                    partitionsSaveDir,
                    latestSaveDir,
                    allEntityIdsAndTypes,
                    dictionaryParser,
                    castEnable,
                    partitioning).migrate(linesToSkip);

        } catch (Throwable th) {
            log.error("Failed to migrate", th);
        }

    }

    private static CommandLine parseArgs(String[] args) {
        Options options = new Options();

        Option telemetryAllFrom = new Option("telemetryFrom", "telemetryFrom", true, "telemetry source file");
        telemetryAllFrom.setRequired(true);
        options.addOption(telemetryAllFrom);

        Option latestTsOutOpt = new Option("latestOut", "latestTelemetryOut", true, "latest telemetry save dir");
        latestTsOutOpt.setRequired(false);
        options.addOption(latestTsOutOpt);

        Option tsOutOpt = new Option("tsOut", "telemetryOut", true, "sstable save dir");
        tsOutOpt.setRequired(false);
        options.addOption(tsOutOpt);

        Option partitionOutOpt = new Option("partitionsOut", "partitionsOut", true, "partitions save dir");
        partitionOutOpt.setRequired(false);
        options.addOption(partitionOutOpt);

        Option castOpt = new Option("castEnable", "castEnable", true, "cast String to Double if possible");
        castOpt.setRequired(true);
        options.addOption(castOpt);

        Option relatedOpt = new Option("relatedEntities", "relatedEntities", true, "related entities source file path");
        relatedOpt.setRequired(true);
        options.addOption(relatedOpt);

        Option dictionaryOpt = new Option("dictionary", "dictionary", true, "dictionary source file path");
        dictionaryOpt.setRequired(true);
        options.addOption(dictionaryOpt);

        Option partitioningOpt = new Option("partitioning", "partitioning", true,
                "Specify partitioning size for timestamp key-value storage. Example: MINUTES, HOURS, DAYS, MONTHS, INDEFINITE");
        partitioningOpt.setRequired(false);
        options.addOption(partitioningOpt);

        Option linesToSkipOpt = new Option("linesToSkip", "linesToSkip", true,
                "Specify number of lines to skip from dump file");
        linesToSkipOpt.setRequired(false);
        options.addOption(linesToSkipOpt);

        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new BasicParser();

        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Parse exception", e);
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }
        return null;
    }

}
