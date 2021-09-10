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

import static org.opensearch.ad.util.RestHandlerUtils.*;
import static org.opensearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.transport.SearchTopAnomalyResultAction;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.SearchTopAnomalyResultRequest;
import org.opensearch.ad.transport.SearchTopAnomalyResultResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestResponseListener;

import com.google.common.collect.ImmutableList;


/**
 * This class consists of the REST handler to search top anomaly results for HC detectors.
 */
public class RestSearchTopAnomalyResultAction extends BaseRestHandler {

    // TODO: confirm if legacy support is needed here or not
    //private static final String LEGACY_URL_PATH = AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI + "/results/_topAnomalies";
    private static final String URL_PATH = String.format(Locale.ROOT, "%s/{%s}/%s/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, RESULTS, TOP_ANOMALIES);
    private final String SEARCH_TOP_ANOMALY_DETECTOR_ACTION = "search_top_anomaly_result";
    private static final Logger logger = LogManager.getLogger(RestSearchTopAnomalyResultAction.class);

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

        // Get the typed request
        SearchTopAnomalyResultRequest searchTopAnomalyResultRequest = getSearchTopAnomalyResultRequest(request);

        // Return the lambda expression (a RestChannelConsumer)
        return channel -> {

            // Validate the request, return errors if found
            String error = validateSearchTopAnomalyResultRequest(searchTopAnomalyResultRequest);
            if (StringUtils.isNotBlank(error)) {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, error));
                return;
            }

            // Execute the correct transport action with the passed-in client, and the rest response listener defined below
            client.execute(SearchTopAnomalyResultAction.INSTANCE, searchTopAnomalyResultRequest, search(channel));
        };
    }

    private SearchTopAnomalyResultRequest getSearchTopAnomalyResultRequest(RestRequest request) throws IOException {
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
                // TODO: see what needs to be built to convert the transport-level response (SearchTopAnomalyResultResponse) to REST-level response (RestResponse)
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

                logger.info("response:\n" + response);

                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), EMPTY_PARAMS));
            }
        };
    }

    private String validateSearchTopAnomalyResultRequest(SearchTopAnomalyResultRequest request) {
        if (request.getStartTime() == null || request.getEndTime() == null) {
            return "Must set both start time and end time with epoch of milliseconds";
        }
        if (!request.getStartTime().isBefore(request.getEndTime())) {
            return "Start time should be before end time";
        }
        if (Strings.isEmpty(request.getDetectorId())) {
            return "Must have a detector ID set";
        }

        // TODO: add more validation on remaining fields
        return null;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(RestRequest.Method.POST, URL_PATH),
                new Route(RestRequest.Method.GET, URL_PATH));
    }
}
