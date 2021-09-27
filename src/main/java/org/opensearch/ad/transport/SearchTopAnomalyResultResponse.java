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

import org.opensearch.action.ActionResponse;
import org.opensearch.ad.model.AnomalyResultBucket;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response for getting the top anomaly results for HC detectors
 */
public class SearchTopAnomalyResultResponse extends ActionResponse implements ToXContentObject {
    private List<AnomalyResultBucket> anomalyResultBuckets;

    public SearchTopAnomalyResultResponse(StreamInput in) throws IOException {
        super(in);
        anomalyResultBuckets = in.readList(AnomalyResultBucket::new);
    }

    public SearchTopAnomalyResultResponse(List<AnomalyResultBucket> anomalyResultBuckets) {
        this.anomalyResultBuckets = anomalyResultBuckets;
    }

    public List<AnomalyResultBucket> getAnomalyResultBuckets() {
        return anomalyResultBuckets;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(anomalyResultBuckets);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(AnomalyResultBucket.BUCKETS_FIELD, anomalyResultBuckets).endObject();
    }
}
