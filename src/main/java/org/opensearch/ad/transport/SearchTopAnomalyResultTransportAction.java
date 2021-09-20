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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Order;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.AnomalyResultBucket;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.ad.model.AnomalyResultBucket;
import org.opensearch.client.Client;

import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;

public class SearchTopAnomalyResultTransportAction extends HandledTransportAction<SearchTopAnomalyResultRequest, SearchTopAnomalyResultResponse> {
    private ADSearchHandler searchHandler;
    private static final String index = ALL_AD_RESULTS_INDEX_PATTERN;
    private static final String COUNT_FIELD = "_count";
    private static final String BUCKET_SORT = "bucket_sort";
    private static final String MULTI_BUCKETS = "multi_buckets";
    private static final Logger logger = LogManager.getLogger(SearchTopAnomalyResultTransportAction.class);
    private final Client client;
    private enum OrderType {
        SEVERITY("severity"),
        OCCURRENCE("occurrence");
        private String name;

        OrderType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    @Inject
    public SearchTopAnomalyResultTransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ADSearchHandler searchHandler,
            Client client
    ) {
        super(SearchTopAnomalyResultAction.NAME, transportService, actionFilters, SearchTopAnomalyResultRequest::new);
        this.searchHandler = searchHandler;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, SearchTopAnomalyResultRequest request, ActionListener<SearchTopAnomalyResultResponse> listener) {

        GetAnomalyDetectorRequest getAdRequest = new GetAnomalyDetectorRequest(request.getDetectorId(), -3L, false, true, "", "", false, null);
        client.execute(GetAnomalyDetectorAction.INSTANCE, getAdRequest, ActionListener.wrap(getAdResponse -> {

            // Make sure detector is HC
            List<String> categoryFieldsFromResponse = getAdResponse.getAnomalyDetector().getCategoryField();
            if (categoryFieldsFromResponse == null || categoryFieldsFromResponse.isEmpty()) {
                throw new IllegalArgumentException(String.format("No category fields found for detector %s", request.getDetectorId()));
            }

            // Get the list of category fields if the user didn't specify any
            if (request.getCategoryFields() == null || request.getCategoryFields().isEmpty()) {
                request.setCategoryFields(categoryFieldsFromResponse);
            } else {
                if (request.getCategoryFields().size() > categoryFieldsFromResponse.size()) {
                    throw new IllegalArgumentException("Too many category fields specified. Please select a subset");
                }
                for (String categoryField : request.getCategoryFields()) {
                    if (!categoryFieldsFromResponse.contains(categoryField)) {
                        throw new IllegalArgumentException(String.format("Category field %s doesn't exist for detector %s", categoryField, request.getDetectorId()));
                    }
                }
            }


            // If we want to retrieve historical results and no task ID was originally passed: make a transport action call
            // to get the latest historical task ID
            if (request.getHistorical() == true && Strings.isNullOrEmpty(request.getTaskId())) {
                ADTask historicalTask = getAdResponse.getHistoricalAdTask();
                if (historicalTask == null) {
                    logger.error("No historical task found");
                    throw new ResourceNotFoundException("No historical task found");
                } else {
                    request.setTaskId(historicalTask.getTaskId());
                }
            }

            // Make sure the order passed is valid. Default to ordering by severity
            OrderType orderType;
            String orderString = request.getOrder();
            if (Strings.isNullOrEmpty(orderString)) {
                orderType = OrderType.SEVERITY;
            } else {
                if (orderString.equals(OrderType.SEVERITY.getName())) {
                    orderType = OrderType.SEVERITY;
                } else if (orderString.equals(OrderType.OCCURRENCE.getName())) {
                    orderType = OrderType.OCCURRENCE;
                } else {
                    // No valid order type was passed, throw an error
                    throw new IllegalArgumentException(String.format("Order %s is not a valid order", orderString));
                }
            }
            request.setOrder(orderType.getName());

            // Generating the search request which will contain the generated query
            SearchRequest searchRequest = generateSearchRequest(request);

            // TODO: remove these 5 lines later. Just for debugging purposes / pretty printing in console
            JsonParser parser = new JsonParser();
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            JsonElement el = parser.parse(searchRequest.source().toString());
            String prettyJson = gson.toJson(el);
            logger.info("\n\nsearch request:\n" + prettyJson);

            searchHandler.search(searchRequest, new TopAnomalyResultListener(listener, 10000, request.getSize()));

        }, exception -> {
            logger.error("Failed to get top anomaly results", exception);
            listener.onFailure(exception);
        }));

    }

    class TopAnomalyResultListener implements ActionListener<SearchResponse> {
        private ActionListener<SearchTopAnomalyResultResponse> listener;
        private long expirationEpochMs;
        private int maxResults;
        private List<AnomalyResultBucket> topResults;

        TopAnomalyResultListener(ActionListener<SearchTopAnomalyResultResponse> listener, long expirationEpochMs, int maxResults) {
            this.listener = listener;
            this.expirationEpochMs = expirationEpochMs;
            this.maxResults = maxResults;
            this.topResults = new ArrayList<>();
        }

        @Override
        public void onResponse(SearchResponse response) {
            /**
             * (from SearchFeatureDAO):
             *
             */
            try {
                Aggregations aggs = response.getAggregations();
                if (aggs == null) {
                    // This would indicate some bug or some opensearch core changes that we are not aware of (we don't keep up-to-date with
                    // the large amounts of changes there). For example, they may change to if there are results return it; otherwise return
                    // null instead of an empty Aggregations as they currently do.
                    logger.warn("Unexpected null aggregation.");
                    listener.onResponse(new SearchTopAnomalyResultResponse(topResults));
                    return;
                }

                Aggregation aggResults = aggs.get(MULTI_BUCKETS);
                if (aggResults == null) {
                    listener.onFailure(new IllegalArgumentException("Failed to find valid aggregation result"));
                    return;
                }

                CompositeAggregation compositeAgg = (CompositeAggregation) aggResults;

                List<AnomalyResultBucket> bucketResults = compositeAgg
                        .getBuckets()
                        .stream()
                        .map(bucket -> AnomalyResultBucket.createAnomalyResultBucket(bucket))
                        .collect(Collectors.toList());

                listener.onResponse(new SearchTopAnomalyResultResponse(bucketResults));

                // TODO: actually iterate through and make sequential calls. Need to figure out how to use updateSourceAfterKey too
//                // we only need at most maxEntitiesForPreview
//                int amountToWrite = maxEntitiesSize - topEntities.size();
//                for (int i = 0; i < amountToWrite && i < pageResults.size(); i++) {
//                    topEntities.add(pageResults.get(i));
//                }
//                Map<String, Object> afterKey = compositeAgg.afterKey();
//                if (topEntities.size() >= maxEntitiesSize || afterKey == null) {
//                    listener.onResponse(topEntities);
//                } else if (expirationEpochMs < clock.millis()) {
//                    if (topEntities.isEmpty()) {
//                        listener.onFailure(new AnomalyDetectionException("timeout to get preview results.  Please retry later."));
//                    } else {
//                        logger.info("timeout to get preview results. Send whatever we have.");
//                        listener.onResponse(topEntities);
//                    }
//                } else {
//                    updateSourceAfterKey(afterKey, searchSourceBuilder);
//                    client
//                            .search(
//                                    new SearchRequest().indices(detector.getIndices().toArray(new String[0])).source(searchSourceBuilder),
//                                    this
//                            );
//                }

            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Failed to paginate top anomaly results", e);
            listener.onFailure(e);
        }
    }

    /**
     * Generates the entire search request
     * @param request the request containing the all of the user-specified parameters needed to generate the request
     * @return the SearchRequest to pass to the SearchHandler
     */
    private SearchRequest generateSearchRequest(SearchTopAnomalyResultRequest request) {
        SearchRequest searchRequest = new SearchRequest().indices(index);

        // Generate the query for the request
        QueryBuilder query = generateQuery(request);

        // Generate the aggregation for the request
        AggregationBuilder aggregation = generateAggregation(request);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    /**
     * Generates the query with appropriate filters on the results indices.
     * If fetching real-time results:
     *  1) term filter on detector_id
     *  2) must_not filter on task_id (because real-time results don't have a 'task_id' field associated with them in the document)
     * If fetching historical results:
     *  1) term filter on the task_id
     * @param request the request containing the detector ID and whether or not to fetch real-time or historical results
     * @return the generated query as a QueryBuilder
     */
    private QueryBuilder generateQuery(SearchTopAnomalyResultRequest request) {
        BoolQueryBuilder query = new BoolQueryBuilder();

        // Add these 2 filters (needed regardless of RT vs. historical)
        RangeQueryBuilder dateRangeFilter = QueryBuilders.rangeQuery(AnomalyResult.EXECUTION_END_TIME_FIELD).gte(request.getStartTime()).lte(request.getEndTime());
        // TODO: remove this later, set to gte for demo
        RangeQueryBuilder anomalyGradeFilter = QueryBuilders.rangeQuery(AnomalyResult.ANOMALY_SCORE_FIELD).gte(0);
        query.filter(dateRangeFilter).filter(anomalyGradeFilter);

        if (request.getHistorical() == true) {
            TermQueryBuilder taskIdFilter = QueryBuilders.termQuery(AnomalyResult.TASK_ID_FIELD, request.getTaskId());
            query.filter(taskIdFilter);
        } else {
            TermQueryBuilder detectorIdFilter = QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, request.getDetectorId());
            ExistsQueryBuilder taskIdFilter = QueryBuilders.existsQuery(AnomalyResult.TASK_ID_FIELD);
            query.filter(detectorIdFilter).mustNot(taskIdFilter);
        }
        return query;
    }

    /**
     * Generates the composite aggregation.
     * Creating a list of sources based on set of user-specified category fields, and sorting on the returned buckets
     *
     * @param request the request containing the all of the user-specified parameters needed to generate the request
     * @return the generated aggregation as an AggregationBuilder
     */
    private AggregationBuilder generateAggregation(SearchTopAnomalyResultRequest request) {
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        for (String categoryField : request.getCategoryFields()) {
            Script script = getScriptForCategoryField(categoryField);
            sources.add(new TermsValuesSourceBuilder(categoryField).script(script));
        }

        // Generate the max anomaly grade aggregation
        AggregationBuilder maxAnomalyGradeAggregation = AggregationBuilders.max(AnomalyResultBucket.MAX_ANOMALY_GRADE_FIELD).field(AnomalyResult.ANOMALY_SCORE_FIELD);

        // Generate the bucket sort aggregation (depends on order type)
        OrderType orderType = request.getOrder().equals(OrderType.SEVERITY.getName()) ? OrderType.SEVERITY : OrderType.OCCURRENCE;
        String sortField = orderType.equals((OrderType.SEVERITY)) ? AnomalyResultBucket.MAX_ANOMALY_GRADE_FIELD : COUNT_FIELD;
        BucketSortPipelineAggregationBuilder bucketSort = PipelineAggregatorBuilders.bucketSort(BUCKET_SORT, new ArrayList<>(Arrays.asList(new FieldSortBuilder(sortField).order(SortOrder.DESC))));

        return AggregationBuilders.composite(MULTI_BUCKETS, sources).subAggregation(maxAnomalyGradeAggregation).subAggregation(bucketSort);
    }

    /**
     * Generates the painless script to fetch results that have an entity name matching the passed-in category field.
     *
     * @param categoryField the category field to be used as a source
     * @return the painless script used to get all docs with entity name values matching the category field
     */
    private Script getScriptForCategoryField(String categoryField) {
        StringBuilder builder = new StringBuilder()
        .append("String value = null;")
        .append("if (params == null || params._source == null || params._source.entity == null) {")
        .append("return \"\"")
        .append("}")
        .append("for (item in params._source.entity) {")
        .append("if (item[\"name\"] == params[\"categoryField\"]) {")
        .append("value = item['value'];")
        .append("break;")
        .append("}")
        .append("}")
        .append("return value;");

        // The last argument contains the K/V pair to inject the categoryField value into the script
        return new Script(ScriptType.INLINE, "painless", builder.toString(), Collections.EMPTY_MAP, ImmutableMap.of("categoryField", categoryField));
    }

}
