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
