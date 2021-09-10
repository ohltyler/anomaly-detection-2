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

package org.opensearch.ad.model;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.ad.annotation.Generated;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.commons.authuser.User;

import com.google.common.base.Objects;

/**
 * Represents a single bucket when retrieving top anomaly results for HC detectors
 */
public class AnomalyResultBucket implements ToXContentObject, Writeable {

    public static final String PARSE_FIELD_NAME = "buckets";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
            AnomalyResultBucket.class,
            new ParseField(PARSE_FIELD_NAME),
            it -> parse(it)
    );

    public static final String KEY_FIELD = "key";
    public static final String DOC_COUNT_FIELD = "doc_count";
    private static final String MAX_ANOMALY_GRADE_FIELD = "max_anomaly_grade";

    private final String key;
    private final int docCount;
    private final double maxAnomalyGrade;

    public AnomalyResultBucket(
            String key,
            int docCount,
            double maxAnomalyGrade
    ) {
        this.key = key;
        this.docCount = docCount;
        this.maxAnomalyGrade = maxAnomalyGrade;
    }


    public AnomalyResultBucket(StreamInput input) throws IOException {
        this.key = input.readString();
        this.docCount = input.readInt();
        this.maxAnomalyGrade = input.readDouble();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
                .startObject()
                .field(KEY_FIELD, key)
                .field(DOC_COUNT_FIELD, docCount)
                .field(MAX_ANOMALY_GRADE_FIELD, maxAnomalyGrade);
        return xContentBuilder.endObject();
    }

    public static AnomalyResultBucket parse(XContentParser parser) throws IOException {
        String key = null;
        Integer docCount = null;
        Double maxAnomalyGrade = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case KEY_FIELD:
                    key = parser.text();
                    break;
                case DOC_COUNT_FIELD:
                    docCount = parser.intValue();
                    break;
                case MAX_ANOMALY_GRADE_FIELD:
                    maxAnomalyGrade = parser.doubleValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new AnomalyResultBucket(
                key,
                docCount,
                maxAnomalyGrade
        );
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AnomalyResultBucket that = (AnomalyResultBucket) o;
        return Objects.equal(getKey(), that.getKey())
                && Objects.equal(getDocCount(), that.getDocCount())
                && Objects.equal(getMaxAnomalyGrade(), that.getMaxAnomalyGrade());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
                .hashCode(
                        getKey(),
                        getDocCount(),
                        getMaxAnomalyGrade()
                );
    }

    @Generated
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("key", key)
                .append("docCount", docCount)
                .append("maxAnomalyGrade", maxAnomalyGrade)
                .toString();
    }

    public String getKey() { return key; }

    public int getDocCount() { return docCount; }

    public double getMaxAnomalyGrade() { return maxAnomalyGrade; }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(key);
        out.writeInt(docCount);
        out.writeDouble(maxAnomalyGrade);
    }
}
