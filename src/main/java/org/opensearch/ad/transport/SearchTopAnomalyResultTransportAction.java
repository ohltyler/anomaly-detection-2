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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.rest.RestSearchTopAnomalyResultAction;
import org.opensearch.ad.transport.SearchTopAnomalyResultRequest;
import org.opensearch.ad.transport.SearchTopAnomalyResultResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.ad.model.AnomalyResultBucket;

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_SEARCH;
import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;
import static org.opensearch.ad.util.ParseUtils.getUserContext;
import static org.opensearch.ad.util.RestHandlerUtils.getSourceContext;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;

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

        // TODO: given the request, generate a SearchRequest to pass to the SearchHandler

        // Match name with existing detectors
        TermsQueryBuilder query = QueryBuilders.termsQuery("name.keyword", "test-detector-name");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
        searchRequest.source(searchSourceBuilder);


        logger.info("\n\nsearch request query:\n" + searchRequest.source().query());


        return searchRequest;
    }


}
