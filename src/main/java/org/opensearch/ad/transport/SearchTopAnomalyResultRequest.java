/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.XContentParser;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Request for getting the top anomaly results for HC detectors.
 *
 * size, category field, and order are optional, and will be set to default values if left blank
 */
public class SearchTopAnomalyResultRequest extends ActionRequest {

    private static final String TASK_ID_FIELD = "task_id";
    private static final String SIZE_FIELD = "size";
    private static final String CATEGORY_FIELD_FIELD = "category_field";
    private static final String ORDER_FIELD = "order";
    private static final String START_TIME_FIELD = "start_time_ms";
    private static final String END_TIME_FIELD = "end_time_ms";
    private String detectorId;
    private String taskId;
    private boolean historical;
    private Integer size;
    private List<String> categoryFields;
    private String order;
    private Instant startTime;
    private Instant endTime;

    public SearchTopAnomalyResultRequest(StreamInput in) throws IOException {
        detectorId = in.readOptionalString();
        taskId = in.readOptionalString();
        historical = in.readBoolean();
        size = in.readOptionalInt();
        categoryFields = in.readOptionalStringList();
        order = in.readOptionalString();
        startTime = in.readInstant();
        endTime = in.readInstant();
    }

    public SearchTopAnomalyResultRequest(
            String detectorId,
            String taskId,
            boolean historical,
            Integer size,
            List<String> categoryFields,
            String order,
            Instant startTime,
            Instant endTime
    )
    {
        super();
        this.detectorId = detectorId;
        this.taskId = taskId;
        this.historical = historical;
        this.size = size;
        this.categoryFields = categoryFields;
        this.order = order;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public String getTaskId() {
        return taskId;
    }

    public boolean getHistorical() { return historical; }

    public Integer getSize() { return size; }

    public List<String> getCategoryFields() { return categoryFields; }

    public String getOrder() { return order; }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void setTaskId (String taskId) { this.taskId = taskId; }

    public void setSize (Integer size) {
        this.size = size;
    }

    public void setCategoryFields (List<String> categoryFields) {
        this.categoryFields = categoryFields;
    }

    public void setOrder (String order) { this.order = order; }

    @SuppressWarnings("unchecked")
    public static SearchTopAnomalyResultRequest parse(XContentParser parser, String detectorId, boolean historical) throws IOException {
        String taskId = null;
        Integer size = null;
        List<Object> categoryFields = null;
        String order = null;
        Instant startTime = null;
        Instant endTime = null;

        // "detectorId" and "historical" params come from the original API path, not in the request body
        // and therefore don't need to be parsed
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case SIZE_FIELD:
                    size = parser.intValue();
                    break;
                case CATEGORY_FIELD_FIELD:
                    categoryFields = parser.list();
                    break;
                case ORDER_FIELD:
                    order = parser.text();
                    break;
                case START_TIME_FIELD:
                    startTime = ParseUtils.toInstant(parser);
                    break;
                case END_TIME_FIELD:
                    endTime = ParseUtils.toInstant(parser);
                    break;
                default:
                    break;
            }
        }

        // Cast category field Object list to String list
        List<String> convertedCategoryFields = (List<String>)(List<?>)(categoryFields);
        return new SearchTopAnomalyResultRequest(detectorId, taskId, historical, size, convertedCategoryFields, order, startTime, endTime);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
