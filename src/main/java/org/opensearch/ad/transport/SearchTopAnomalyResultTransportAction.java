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

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.common.inject.Inject;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.ad.model.AnomalyResultBucket;

import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;

public class SearchTopAnomalyResultTransportAction extends HandledTransportAction<SearchTopAnomalyResultRequest, SearchTopAnomalyResultResponse> {
    private ADSearchHandler searchHandler;
    private static final String index = ALL_AD_RESULTS_INDEX_PATTERN;
    private static final Logger logger = LogManager.getLogger(SearchTopAnomalyResultTransportAction.class);

    @Inject
    public SearchTopAnomalyResultTransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ADSearchHandler searchHandler
    ) {
        super(SearchTopAnomalyResultAction.NAME, transportService, actionFilters, SearchTopAnomalyResultRequest::new);
        this.searchHandler = searchHandler;
    }

    @Override
    protected void doExecute(Task task, SearchTopAnomalyResultRequest request, ActionListener<SearchTopAnomalyResultResponse> listener) {

        // Generating the search request which will contain the generated query
        SearchRequest searchRequest = generateSearchRequest(request);

        // Creating a listener of appropriate type to pass to the SearchHandler
        ActionListener<SearchResponse> searchListener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {

                // TODO: convert the searchResponse back to SearchTopAnomalyResultResponse

                List<AnomalyResultBucket> results = new ArrayList<AnomalyResultBucket>();
                SearchTopAnomalyResultResponse response = new SearchTopAnomalyResultResponse(results);
                listener.onResponse(response);
                return;
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
                return;
            }
        };

        // Pass the request to the existing SearchHandler
        searchHandler.search(searchRequest, searchListener);
    }

    private SearchRequest generateSearchRequest(SearchTopAnomalyResultRequest request) {
        SearchRequest searchRequest = new SearchRequest().indices(index);

        // Generate the query for the request
        QueryBuilder query = generateQuery(request);

        // Generate the aggregation for the request
        AggregationBuilder aggregation = generateAggregation(request);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);

        // TODO: remove these 5 lines later. Just for debugging purposes / pretty printing in console
        JsonParser parser = new JsonParser();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonElement el = parser.parse(searchRequest.source().toString());
        String prettyJson = gson.toJson(el);
        logger.info("\n\nsearch request:\n" + prettyJson);

        return searchRequest;
    }

    private QueryBuilder generateQuery(SearchTopAnomalyResultRequest request) {
        QueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("detector_id", request.getDetectorId()));

        // TODO: add filter(s) for historical v. realtime
        /**
         *
         * Currently, when calling getDetector with task=true we can get the latest RT task ("realtime_detection_task")
         * and latest historical task ("historical_analysis_task")
         *
         * Currently, RT results are stored without a task ID, even though there is an underlying RT task running.
         * This is for BWC consistency purposes.
         * Historical results are stored with an additional task ID.
         *
         * To differentiate on the frontend, we have to pass different filters for fetching RT v. historical results:
         * RT:
         *   1) term filter on detector id: e.g., "detector_id": <detector-id>
         *   2) must_not exists filter on "task_id" (so we filter out historical results that have detector_id + task_id)
         *
         * Historical:
         *   1) get latest task id (stored in "historical_analysis_task" when calling get detector API)
         *   2) term filter on that task id: e.g., "task_id": <task-id>
         *
         * So, we need to have different filters here based on if the user wants RT v. historical data
         * RT: add the same 2 filters that the frontend does
         * Historical: make a get detector transport action call to fetch stored task ID, use returned id in a task_id filter
         */

        return query;
    }

    // Generates a composite aggregation, with the sources being the different
    // category fields the user wants to filter by
    private AggregationBuilder generateAggregation(SearchTopAnomalyResultRequest request) {
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources.add(new TermsValuesSourceBuilder("field1").field("field1"));
        AggregationBuilder aggregation = AggregationBuilders.composite("multi_buckets", sources);


        return aggregation;
    }


}
