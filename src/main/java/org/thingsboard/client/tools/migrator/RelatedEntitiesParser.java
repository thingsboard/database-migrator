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
