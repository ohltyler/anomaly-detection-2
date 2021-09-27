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

package org.opensearch.ad.transport;

import org.junit.Test;
import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.ad.constant.CommonName;

import java.util.Arrays;
import java.util.ArrayList;
import java.time.Instant;
import java.util.List;

public class SearchTopAnomalyResultActionTests extends ADIntegTestCase {

//    @Test
//    public void testSearchResultAction() throws IOException {
//        createADResultIndex();
//        String adResultId = createADResult(TestHelpers.randomAnomalyDetectResult());
//
//        SearchResponse searchResponse = client().execute(SearchAnomalyResultAction.INSTANCE, matchAllRequest()).actionGet(10000);
//        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value);
//
//        assertEquals(adResultId, searchResponse.getInternalResponse().hits().getAt(0).getId());
//    }

    @Test
    public void testNoIndex() {
        deleteIndexIfExists(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        List<String> categoryFields = new ArrayList<>(Arrays.asList("test-category-field"));
        SearchTopAnomalyResultRequest req = new SearchTopAnomalyResultRequest(
                "test-detector-id",
                "test-task-id",
                false,
                10,
                categoryFields,
                "severity",
                Instant.now().minusMillis(100_000),
                Instant.now()
                );
        SearchTopAnomalyResultResponse response = client().execute(SearchTopAnomalyResultAction.INSTANCE, req).actionGet(10000);
        assertEquals(0, response.getAnomalyResultBuckets().size());
    }

}
