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
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Request for getting the top anomaly results for HC detectors.
 *
 * size, category field, and order are optional, and will be set to default values if left blank
 */
public class SearchTopAnomalyResultRequest extends ActionRequest implements ToXContentObject {

    private static final String DETECTOR_ID_FIELD = "detector_id";
    private static final String HISTORICAL_FIELD = "historical";
    private static final String SIZE_FIELD = "size";
    private static final String CATEGORY_FIELD_FIELD = "category_field";
    private static final String ORDER_FIELD = "order";
    private static final String START_TIME_FIELD = "start_time_ms";
    private static final String END_TIME_FIELD = "end_time_ms";
    private String detectorId;
    private boolean historical;
    private int size;
    private List<String> categoryFields;
    private String order;
    private Instant startTime;
    private Instant endTime;

    public SearchTopAnomalyResultRequest(StreamInput in) throws IOException {
        detectorId = in.readOptionalString();
        historical = in.readBoolean();
        size = in.readOptionalInt();
        categoryFields = in.readOptionalStringList();
        order = in.readOptionalString();
        startTime = in.readInstant();
        endTime = in.readInstant();
    }

    public SearchTopAnomalyResultRequest(String detectorId, boolean historical, int size, List<String> categoryFields, String order, Instant startTime, Instant endTime)
            throws IOException {
        super();
        this.detectorId = detectorId;
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

    public boolean getHistorical() { return historical; }

    public int getSize() { return size; }

    public List<String> getCategoryFields() { return categoryFields; }

    public String getOrder() { return order; }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
                .startObject()
                .field(DETECTOR_ID_FIELD, detectorId)
                .field(HISTORICAL_FIELD, historical)
                .field(SIZE_FIELD, size)
                .field(CATEGORY_FIELD_FIELD, categoryFields)
                .field(ORDER_FIELD, order)
                .field(START_TIME_FIELD, startTime.toEpochMilli())
                .field(END_TIME_FIELD, endTime.toEpochMilli());
        return xContentBuilder.endObject();
    }

    @SuppressWarnings("unchecked")
    public static SearchTopAnomalyResultRequest parse(XContentParser parser, String detectorId, boolean historical) throws IOException {
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

        return new SearchTopAnomalyResultRequest(detectorId, historical, size, convertedCategoryFields, order, startTime, endTime);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(detectorId);
        out.writeOptionalBoolean(historical);
        out.writeOptionalInt(size);
        out.writeOptionalStringCollection(categoryFields);
        out.writeInstant(startTime);
        out.writeInstant(endTime);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
