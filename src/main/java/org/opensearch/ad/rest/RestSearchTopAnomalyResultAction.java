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

package org.opensearch.ad.rest;

import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;
import static org.opensearch.ad.util.RestHandlerUtils.*;
import static org.opensearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.transport.SearchTopAnomalyResultAction;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.DeleteAnomalyResultsAction;
import org.opensearch.ad.transport.SearchTopAnomalyResultRequest;
import org.opensearch.ad.transport.SearchTopAnomalyResultResponse;
import org.opensearch.ad.transport.SearchTopAnomalyResultTransportAction;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.google.common.collect.ImmutableList;

import com.google.common.collect.ImmutableList;
import org.opensearch.rest.BaseRestHandler;

/**
 * This class consists of the REST handler to search top anomaly results for HC detectors.
 */
public class RestSearchTopAnomalyResultAction extends BaseRestHandler {

    // TODO: confirm if legacy support is needed here or not
    //private static final String LEGACY_URL_PATH = AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI + "/results/_topAnomalies";
    private static final String URL_PATH = String.format(Locale.ROOT, "%s/{%s}/%s/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, RESULTS, TOP_ANOMALIES);
    private final String SEARCH_TOP_ANOMALY_DETECTOR_ACTION = "search_top_anomaly_result";
    private static final Logger logger = LogManager.getLogger(RestSearchTopAnomalyResultAction.class);
    private static final String index = ALL_AD_RESULTS_INDEX_PATTERN;
    private static final Class clazz = AnomalyResult.class;

    public RestSearchTopAnomalyResultAction() {}

    @Override
    public String getName() {
        return SEARCH_TOP_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        // Throw error if disabled
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param(DETECTOR_ID);
        logger.log(Level.INFO, detectorId);


        // Build the request
        SearchTopAnomalyResultRequest searchTopAnomalyResultRequest = parseRequest(request);

        // Build the query
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser());
        searchSourceBuilder.fetchSource(getSourceContext(request));
        searchSourceBuilder.seqNoAndPrimaryTerm(true).version(true);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(index);
        logger.log(Level.DEBUG, searchRequest.toString());

        // Execute the correct transport action with the passed-in client, and the rest response listener defined below
        return channel -> client.execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest, search(channel));
    }

    private SearchTopAnomalyResultRequest parseRequest(RestRequest request) throws IOException {
        String detectorId = null;
        if (request.hasParam(DETECTOR_ID)) {
            detectorId = request.param(DETECTOR_ID);
        }

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        SearchTopAnomalyResultRequest req = SearchTopAnomalyResultRequest.parse(parser, detectorId);
        logger.log(Level.INFO, req.getDetectorId());
        logger.log(Level.INFO, req.getSize());
        logger.log(Level.INFO, req.getCategoryFields());
        logger.log(Level.INFO, req.getOrder());
        logger.log(Level.INFO, req.getStartTime());
        logger.log(Level.INFO, req.getEndTime());
        return req;
    }


    // Rest response listener to handle the response
    private RestResponseListener<SearchTopAnomalyResultResponse> search(RestChannel channel) {
        return new RestResponseListener<SearchTopAnomalyResultResponse>(channel) {
            @Override
            public RestResponse buildResponse(SearchTopAnomalyResultResponse response) throws Exception {
                // TODO: see what needs to be built to convert the transport-level response (SearchTopAnomalyResultResponse)
                // into a REST-level response (RestResponse)
//                if (response.isTimedOut()) {
//                    return new BytesRestResponse(RestStatus.REQUEST_TIMEOUT, response.toString());
//                }
//
//                if (clazz == AnomalyDetector.class) {
//                    for (SearchHit hit : response.getHits()) {
//                        XContentParser parser = XContentType.JSON
//                                .xContent()
//                                .createParser(
//                                        channel.request().getXContentRegistry(),
//                                        LoggingDeprecationHandler.INSTANCE,
//                                        hit.getSourceAsString()
//                                );
//                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
//
//                        // write back id and version to anomaly detector object
//                        ToXContentObject xContentObject = AnomalyDetector.parse(parser, hit.getId(), hit.getVersion());
//                        XContentBuilder builder = xContentObject.toXContent(jsonBuilder(), EMPTY_PARAMS);
//                        hit.sourceRef(BytesReference.bytes(builder));
//                    }
//                }

                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), EMPTY_PARAMS));
            }
        };
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(RestRequest.Method.POST, URL_PATH),
                new Route(RestRequest.Method.GET, URL_PATH));
    }
}
