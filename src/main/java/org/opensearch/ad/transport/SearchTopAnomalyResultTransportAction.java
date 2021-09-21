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

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
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
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.AnomalyResultBucket;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.Strings;
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
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.client.Client;

import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;

/**
 * Transport action to fetch top anomaly results for some HC detector. Generates a query based on user input to fetch
 * aggregated entity results.
 *
 * Example of a generated query aggregating over the "Carrier" category field, and sorting on max anomaly grade, using
 * a historical task ID:
 *
 * {
 *   "query": {
 *     "bool": {
 *       "filter": [
 *         {
 *           "range": {
 *             "execution_end_time": {
 *               "from": "2021-09-10T07:00:00.000Z",
 *               "to": "2021-09-30T07:00:00.000Z",
 *               "include_lower": true,
 *               "include_upper": true,
 *               "boost": 1.0
 *             }
 *           }
 *         },
 *         {
 *           "range": {
 *             "anomaly_score": {
 *               "from": 0,
 *               "include_lower": true,
 *               "include_upper": true,
 *               "boost": 1.0
 *             }
 *           }
 *         },
 *         {
 *           "term": {
 *             "task_id": {
 *               "value": "2AwACXwBM-RcgLq7Za87",
 *               "boost": 1.0
 *             }
 *           }
 *         }
 *       ],
 *       "adjust_pure_negative": true,
 *       "boost": 1.0
 *     }
 *   },
 *   "aggregations": {
 *     "multi_buckets": {
 *       "composite": {
 *         "size": 100,
 *         "sources": [
 *           {
 *             "Carrier": {
 *               "terms": {
 *                 "script": {
 *                   "source": "String value = null;if (params == null || params._source == null || params._source.entity == null) {return \"\"}for (item in params._source.entity) {if (item[\"name\"] == params[\"categoryField\"]) {value = item['value'];break;}}return value;",
 *                   "lang": "painless",
 *                   "params": {
 *                     "categoryField": "Carrier"
 *                   }
 *                 },
 *                 "missing_bucket": false,
 *                 "order": "asc"
 *               }
 *             }
 *           }
 *         ]
 *       },
 *       "aggregations": {
 *         "max_anomaly_grade": {
 *           "max": {
 *             "field": "anomaly_score"
 *           }
 *         },
 *         "bucket_sort": {
 *           "bucket_sort": {
 *             "sort": [
 *               {
 *                 "max_anomaly_grade": {
 *                   "order": "desc"
 *                 }
 *               }
 *             ],
 *             "from": 0,
 *             "gap_policy": "SKIP"
 *           }
 *         }
 *       }
 *     }
 *   }
 * }
 */
public class SearchTopAnomalyResultTransportAction extends HandledTransportAction<SearchTopAnomalyResultRequest, SearchTopAnomalyResultResponse> {
    private ADSearchHandler searchHandler;
    // TODO: tune this to be a reasonable timeout
    private static final long TIMEOUT_IN_MILLIS = 30000;
    // Number of buckets to return per page
    private static final int PAGE_SIZE = 100;
    private static final OrderType DEFAULT_ORDER_TYPE = OrderType.SEVERITY;
    private static final int DEFAULT_SIZE = 10;
    private static final String index = ALL_AD_RESULTS_INDEX_PATTERN;
    private static final String COUNT_FIELD = "_count";
    private static final String BUCKET_SORT = "bucket_sort";
    private static final String MULTI_BUCKETS = "multi_buckets";
    private static final Logger logger = LogManager.getLogger(SearchTopAnomalyResultTransportAction.class);
    private final Client client;
    private Clock clock;
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
        this.clock = Clock.systemUTC();
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


            // If we want to retrieve historical results and no task ID was originally passed: use the latest
            // task ID stored in the detector's historical task
            if (request.getHistorical() == true && Strings.isNullOrEmpty(request.getTaskId())) {
                ADTask historicalTask = getAdResponse.getHistoricalAdTask();
                if (historicalTask == null) {
                    logger.error("No historical task found");
                    throw new ResourceNotFoundException("No historical task found");
                } else {
                    request.setTaskId(historicalTask.getTaskId());
                }
            }

            // Make sure the order passed is valid. If nothing passed use default
            OrderType orderType;
            String orderString = request.getOrder();
            if (Strings.isNullOrEmpty(orderString)) {
                orderType = DEFAULT_ORDER_TYPE;
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

            // Make sure the size passed is valid. If nothing passed use default
            if (request.getSize() == null) {
                request.setSize(DEFAULT_SIZE);
            } else if (request.getSize() <= 0) {
                throw new IllegalArgumentException("Size field must be a positive integer");
            }

            // Generating the search request which will contain the generated query
            SearchRequest searchRequest = generateSearchRequest(request);

            // TODO: remove these 5 lines later. Just for debugging purposes / pretty printing in console
            JsonParser parser = new JsonParser();
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            JsonElement el = parser.parse(searchRequest.source().toString());
            String prettyJson = gson.toJson(el);
            logger.info("\n\nsearch request:\n" + prettyJson);

            searchHandler.search(searchRequest, new TopAnomalyResultListener(listener,
                    searchRequest.source(),
                    clock.millis() + TIMEOUT_IN_MILLIS,
                    request.getSize(),
                    orderType));

        }, exception -> {
            logger.error("Failed to get top anomaly results", exception);
            listener.onFailure(exception);
        }));

    }

    /**
     * ActionListener class to handle bucketed search results in a paginated fashion.
     * Note that the bucket_sort aggregation is a pipeline aggregation, and is executed
     * after all non-pipeline aggregations (including the composite bucket aggregation).
     * Because of this, the sorting is only done locally based on the buckets in the current page,
     * and does not recognize buckets from other pages. To get around this issue, we use a max
     * heap and add all results to the heap until there are no more buckets. This guarantees
     * that the final set of buckets in the heap is the globally sorted set of buckets.
     */
    class TopAnomalyResultListener implements ActionListener<SearchResponse> {
        private ActionListener<SearchTopAnomalyResultResponse> listener;
        SearchSourceBuilder searchSourceBuilder;
        private long expirationEpochMs;
        private int maxResults;
        private OrderType orderType;
        private PriorityQueue<AnomalyResultBucket> maxHeap;

        TopAnomalyResultListener(ActionListener<SearchTopAnomalyResultResponse> listener,
                                 SearchSourceBuilder searchSourceBuilder,
                                 long expirationEpochMs,
                                 int maxResults,
                                 OrderType orderType) {
            this.listener = listener;
            this.searchSourceBuilder = searchSourceBuilder;
            this.expirationEpochMs = expirationEpochMs;
            this.maxResults = maxResults;
            this.orderType = orderType;
            this.maxHeap = new PriorityQueue<>(maxResults, new Comparator<AnomalyResultBucket>() {
                // Sorting by descending order of anomaly grade or doc count
                @Override
                public int compare(AnomalyResultBucket bucket1, AnomalyResultBucket bucket2) {
                    if (orderType == OrderType.SEVERITY) {
                        double difference = bucket1.getMaxAnomalyGrade() - bucket2.getMaxAnomalyGrade();
                        return difference > 0 ? 1 : difference == 0 ? 0 : -1;
                    } else {
                        return bucket1.getDocCount() - bucket2.getDocCount();
                    }
                }
            });
        }

        @Override
        public void onResponse(SearchResponse response) {
            try {
                Aggregations aggs = response.getAggregations();
                if (aggs == null) {
                    // This would indicate some bug or some opensearch core changes that we are not aware of (we don't keep up-to-date with
                    // the large amounts of changes there). For example, they may change to if there are results return it; otherwise return
                    // null instead of an empty Aggregations as they currently do.
                    logger.warn("Unexpected null aggregation.");
                    listener.onResponse(new SearchTopAnomalyResultResponse(new ArrayList<>()));
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

                // Add all of the results to the heap, and only keep the top maxResults buckets
                maxHeap.addAll(bucketResults);
                while (maxHeap.size() > maxResults) {
                    maxHeap.poll();
                }

                Map<String, Object> afterKey = compositeAgg.afterKey();

                // If afterKey is null: we've hit the end of results. Return the results
                if (afterKey == null) {
                    listener.onResponse(new SearchTopAnomalyResultResponse(getOrderedListFromMaxHeap(maxHeap, orderType)));
                } else if (expirationEpochMs < clock.millis()) {
                    if (maxHeap.isEmpty()) {
                        listener.onFailure(new AnomalyDetectionException("Timed out getting all top anomaly results. Please retry later."));
                    } else {
                        logger.info("Timed out getting all top anomaly results. Sending back partial results.");
                        listener.onResponse(new SearchTopAnomalyResultResponse(getOrderedListFromMaxHeap(maxHeap, orderType)));
                    }
                } else {
                    CompositeAggregationBuilder aggBuilder = (CompositeAggregationBuilder) searchSourceBuilder.aggregations().
                            getAggregatorFactories().iterator().next();
                    aggBuilder.aggregateAfter(afterKey);

                    // Searching more, using an updated source with an after_key
                    searchHandler
                            .search(
                                    new SearchRequest().indices(index).source(searchSourceBuilder),
                                    this
                            );
                }

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
        AggregationBuilder maxAnomalyGradeAggregation = AggregationBuilders
                .max(AnomalyResultBucket.MAX_ANOMALY_GRADE_FIELD)
                .field(AnomalyResult.ANOMALY_SCORE_FIELD);

        // Generate the bucket sort aggregation (depends on order type)
        OrderType orderType = request.getOrder().equals(OrderType.SEVERITY.getName()) ? OrderType.SEVERITY : OrderType.OCCURRENCE;
        String sortField = orderType.equals((OrderType.SEVERITY)) ? AnomalyResultBucket.MAX_ANOMALY_GRADE_FIELD : COUNT_FIELD;
        BucketSortPipelineAggregationBuilder bucketSort = PipelineAggregatorBuilders
                .bucketSort(BUCKET_SORT, new ArrayList<>(Arrays.asList(new FieldSortBuilder(sortField).order(SortOrder.DESC))));

        return AggregationBuilders.composite(MULTI_BUCKETS, sources).size(PAGE_SIZE).subAggregation(maxAnomalyGradeAggregation).subAggregation(bucketSort);
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

    /**
     * Creates an ordered List from the max heap, based on the order type.
     *
     * @param maxHeap the max heap
     * @param orderType the order type to determine how to sort the result buckets
     * @return an ordered List containing all of the elements in the max heap
     */
    private List<AnomalyResultBucket> getOrderedListFromMaxHeap(PriorityQueue<AnomalyResultBucket> maxHeap, OrderType orderType) {
        List<AnomalyResultBucket> maxHeapAsList = new ArrayList<>(maxHeap);
        Collections.sort(maxHeapAsList, new Comparator<AnomalyResultBucket>() {
            @Override
            public int compare(AnomalyResultBucket bucket1, AnomalyResultBucket bucket2) {
                if (orderType == OrderType.SEVERITY) {
                    double difference = bucket2.getMaxAnomalyGrade() - bucket1.getMaxAnomalyGrade();
                    return difference > 0 ? 1 : difference == 0 ? 0 : -1;
                } else {
                    return bucket2.getDocCount() - bucket1.getDocCount();
                }
            }
        });
        return maxHeapAsList;
    }

}
