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
import org.thingsboard.server.common.data.EntityType;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class RelatedEntitiesParser {
    private final Map<String, String> allEntityIdsAndTypes = new HashMap<>();

    private Map<String, EntityType> tableNameAndEntityType;

    public RelatedEntitiesParser(File source) throws IOException {
        tableNameAndEntityType =
                new HashMap<>();
        tableNameAndEntityType.put("COPY public.alarm ", EntityType.ALARM);
        tableNameAndEntityType.put("COPY public.asset ", EntityType.ASSET);
        tableNameAndEntityType.put("COPY public.customer ", EntityType.CUSTOMER);
        tableNameAndEntityType.put("COPY public.dashboard ", EntityType.DASHBOARD);
        tableNameAndEntityType.put("COPY public.device ", EntityType.DEVICE);
        tableNameAndEntityType.put("COPY public.rule_chain ", EntityType.RULE_CHAIN);
        tableNameAndEntityType.put("COPY public.rule_node ", EntityType.RULE_NODE);
        tableNameAndEntityType.put("COPY public.tenant ", EntityType.TENANT);
        tableNameAndEntityType.put("COPY public.tb_user ", EntityType.USER);
        tableNameAndEntityType.put("COPY public.entity_view ", EntityType.ENTITY_VIEW);
        tableNameAndEntityType.put("COPY public.widgets_bundle ", EntityType.WIDGETS_BUNDLE);
        tableNameAndEntityType.put("COPY public.widget_type ", EntityType.WIDGET_TYPE);
        tableNameAndEntityType.put("COPY public.tenant_profile ", EntityType.TENANT_PROFILE);
        tableNameAndEntityType.put("COPY public.device_profile ", EntityType.DEVICE_PROFILE);
        tableNameAndEntityType.put("COPY public.asset_profile ", EntityType.ASSET_PROFILE);
        tableNameAndEntityType.put("COPY public.api_usage_state ", EntityType.API_USAGE_STATE);

        // PE
        tableNameAndEntityType.put("COPY public.entity_group ", EntityType.ENTITY_GROUP);
        tableNameAndEntityType.put("COPY public.converter ", EntityType.CONVERTER);
        tableNameAndEntityType.put("COPY public.integration ", EntityType.INTEGRATION);
        tableNameAndEntityType.put("COPY public.scheduler_event ", EntityType.SCHEDULER_EVENT);
        tableNameAndEntityType.put("COPY public.blob_entity ", EntityType.BLOB_ENTITY);
        tableNameAndEntityType.put("COPY public.role ", EntityType.ROLE);
        tableNameAndEntityType.put("COPY public.group_permission ", EntityType.GROUP_PERMISSION);
        tableNameAndEntityType.put("COPY public.resource ", EntityType.TB_RESOURCE);
        tableNameAndEntityType.put("COPY public.ota_package ", EntityType.OTA_PACKAGE);
        tableNameAndEntityType.put("COPY public.edge ", EntityType.EDGE);
        tableNameAndEntityType.put("COPY public.rpc ", EntityType.RPC);

        processAllTables(FileUtils.lineIterator(source));
    }

    public String getEntityType(String uuid) {
        return this.allEntityIdsAndTypes.get(uuid);
    }

    private void processAllTables(LineIterator lineIterator) throws IOException {
        String currentLine;
        try {
            while (lineIterator.hasNext()) {
                currentLine = lineIterator.nextLine();
                for (Map.Entry<String, EntityType> entry : tableNameAndEntityType.entrySet()) {
                    if (currentLine.startsWith(entry.getKey())) {
                        int idIdx = getIdIdx(currentLine);
                        processBlock(lineIterator, entry.getValue(), idIdx);
                    }
                }
            }
        } finally {
            lineIterator.close();
        }
    }

    private int getIdIdx(String headerLine) {
        log.info("Going to process next headerLine: {}", headerLine);
        String columns = headerLine.substring(headerLine.indexOf("(") + 1, headerLine.indexOf(")"));
        String[] split = columns.split(", ");
        int idx = 0;
        for (String s : split) {
            if ("id".equalsIgnoreCase(s)) {
                return idx;
            }
            idx++;
        }
        throw new RuntimeException("ID column is not present in this table :" + headerLine);
    }

    private void processBlock(LineIterator lineIterator, EntityType entityType, int idIdx) {
        String currentLine;
        while (lineIterator.hasNext()) {
            currentLine = lineIterator.nextLine();
            if (WriterUtils.isBlockFinished(currentLine)) {
                return;
            }
            allEntityIdsAndTypes.put(currentLine.split("\t")[idIdx], entityType.name());
        }
    }
}
