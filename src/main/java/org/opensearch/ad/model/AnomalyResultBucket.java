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

import java.io.IOException;
import java.util.Map;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;

/**
 * Represents a single bucket when retrieving top anomaly results for HC detectors
 */
public class AnomalyResultBucket implements ToXContentObject, Writeable {
    public static final String BUCKETS_FIELD = "buckets";
    public static final String KEY_FIELD = "key";
    public static final String DOC_COUNT_FIELD = "doc_count";
    public static final String MAX_ANOMALY_GRADE_FIELD = "max_anomaly_grade";

    private final Map<String, Object> key;
    private final int docCount;
    private final double maxAnomalyGrade;

    public AnomalyResultBucket(
            Map<String, Object> key,
            int docCount,
            double maxAnomalyGrade
    ) {
        this.key = key;
        this.docCount = docCount;
        this.maxAnomalyGrade = maxAnomalyGrade;
    }


    public AnomalyResultBucket(StreamInput input) throws IOException {
        this.key = input.readMap();
        this.docCount = input.readInt();
        this.maxAnomalyGrade = input.readDouble();
    }

    public static AnomalyResultBucket createAnomalyResultBucket(Bucket bucket) {
        return new AnomalyResultBucket(bucket.getKey(), (int) bucket.getDocCount(), ((InternalMax) bucket.getAggregations().get(MAX_ANOMALY_GRADE_FIELD)).getValue());
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(key);
        out.writeInt(docCount);
        out.writeDouble(maxAnomalyGrade);
    }

    public Map<String, Object> getKey() { return key; }

    public int getDocCount() { return docCount; }

    public double getMaxAnomalyGrade() { return maxAnomalyGrade; }
}
