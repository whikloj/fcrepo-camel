/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.camel;

import static java.lang.Boolean.FALSE;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;
import static org.apache.camel.Exchange.CONTENT_TYPE;
import static org.apache.camel.Exchange.DISABLE_HTTP_STREAM_CACHE;
import static org.apache.camel.Exchange.HTTP_METHOD;
import static org.apache.camel.Exchange.HTTP_RESPONSE_CODE;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.LOCATION;
import static org.fcrepo.camel.FcrepoConstants.FIXITY;
import static org.fcrepo.camel.FcrepoHeaders.FCREPO_BASE_URL;
import static org.fcrepo.camel.FcrepoHeaders.FCREPO_IDENTIFIER;
import static org.fcrepo.camel.FcrepoHeaders.FCREPO_PREFER;
import static org.fcrepo.camel.FcrepoHeaders.FCREPO_URI;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.converter.stream.CachedOutputStream;
import org.apache.camel.support.DefaultProducer;
import org.apache.camel.support.ExchangeHelper;
import org.apache.camel.util.IOHelper;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The Fedora producer.
 *
 * @author Aaron Coburn
 * @since October 20, 2014
 */
public class FcrepoProducer extends DefaultProducer {

    public static final String DEFAULT_CONTENT_TYPE = "application/rdf+xml";

    private static final Logger LOGGER = getLogger(FcrepoProducer.class);

    private static final String LDP = "http://www.w3.org/ns/ldp#";

    private static final String REPOSITORY = "http://fedora.info/definitions/v4/repository#";

    private static final String FEDORA_API = "http://fedora.info/definitions/fcrepo#";

    private final FcrepoEndpoint endpoint;

    private CloseableHttpClient httpClient;

    private HttpClientContext httpContext;

    private final TransactionTemplate transactionTemplate;

    public static final Map<String, String> PREFER_PROPERTIES;

    static {
        final Map<String, String> prefer = new HashMap<>();
        prefer.put("PreferContainment", LDP + "PreferContainment");
        prefer.put("PreferMembership", LDP + "PreferMembership");
        prefer.put("PreferMinimalContainer", LDP + "PreferMinimalContainer");
        prefer.put("ServerManaged", REPOSITORY + "ServerManaged");
        prefer.put("EmbedResources", "http://www.w3.org/ns/oa#PreferContainedDescriptions");
        prefer.put("InboundReferences", FEDORA_API + "InboundReferences");

        PREFER_PROPERTIES = unmodifiableMap(prefer);
    }

    /**
     *  Add the appropriate namespace to the prefer header in case the
     *  short form was supplied.
     */
    private static Function<String, String> addPreferNamespace = property -> {
        final String prefer = PREFER_PROPERTIES.get(property);
        if (!isBlank(prefer)) {
            return prefer;
        }
        return property;
    };


    /**
     * Create a FcrepoProducer object
     *
     * @param endpoint the FcrepoEndpoint corresponding to the exchange.
     */
    public FcrepoProducer(final FcrepoEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
        this.transactionTemplate = endpoint.createTransactionTemplate();
        this.httpClient = endpoint.getHttpClient();
        this.httpContext = endpoint.getHttpContext();
    }

    /**
     * Define how message exchanges are processed.
     *
     * @param exchange the InOut message exchange
     * @throws IOException when the underlying HTTP request results in an error
     */
    @Override
    public void process(final Exchange exchange) throws FcrepoHttpOperationFailedException, IOException {
        if (exchange.isTransacted()) {
            transactionTemplate.execute(new TransactionCallbackWithoutResult() {
                @Override
                protected void doInTransactionWithoutResult(final TransactionStatus status) {
                    final DefaultTransactionStatus st = (DefaultTransactionStatus)status;
                    final FcrepoTransactionObject tx = (FcrepoTransactionObject)st.getTransaction();
                    try {
                        doRequest(exchange, tx.getSessionId());
                    } catch (final IOException | FcrepoHttpOperationFailedException ex) {
                        throw new TransactionSystemException(
                            "Error executing fcrepo request in transaction: ", ex);
                    }
                }
            });
        } else {
            doRequest(exchange, null);
        }
    }

    private void doRequest(final Exchange exchange, final String transaction) throws FcrepoHttpOperationFailedException,
            IOException {
        final Message in = exchange.getIn();
        final String method = getMethod(exchange);
        final String contentType = getContentType(exchange);
        final String accept = getAccept(exchange);
        final String url = getUrl(exchange, transaction);

        LOGGER.debug("Request [{}] with method [{}]", url, method);

        try {
            final HttpRequestBase request;
            switch (method) {
                case "PATCH":
                    request = new HttpPatch(getMetadataUri(url));
                    break;
                case "PUT":
                    request = new HttpPut(url);
                    break;
                case "POST":
                    request = new HttpPost(url);
                    break;
                case "DELETE":
                    request = new HttpDelete(url);
                    break;
                case "HEAD":
                    request = new HttpHead(url);
                    break;
                case "GET":
                default:
                    request = new HttpGet(getUri(endpoint, url));
                    request.setHeader(ACCEPT, accept);
                    final String preferHeader = in.getHeader(FCREPO_PREFER, "", String.class);
                    if (!preferHeader.isEmpty()) {
                        request.setHeader("Prefer", preferHeader);
                    } else {
                        final String endpointPreferHeader = buildPreferHeader();
                        if (endpointPreferHeader != null) {
                            request.setHeader("Prefer", endpointPreferHeader);
                        }
                    }
            }
            LOGGER.debug("request to uri {} is of type {}", request.getURI(), request.getMethod());
            if (request instanceof HttpEntityEnclosingRequestBase) {
                final InputStream sourceBody = in.getBody(InputStream.class);
                if (sourceBody != null) {
                    LOGGER.debug("source is not null");
                    if (contentType != null) {
                        request.setHeader(CONTENT_TYPE, contentType);
                    } else if (request instanceof HttpPatch) {
                        request.setHeader(CONTENT_TYPE, "application/sparql-update");
                    }
                    ((HttpEntityEnclosingRequestBase) request).setEntity(new InputStreamEntity(sourceBody));
                }
            }
            LOGGER.debug("request is {}", request.toString());
            try (final CloseableHttpResponse resp = httpClient.execute(request, httpContext)) {
                if (!isValidResponse.test(resp) && endpoint.getThrowExceptionOnFailure()) {
                    throw new FcrepoHttpOperationFailedException(request.getURI(), resp.getStatusLine().getStatusCode(),
                            getEntityBodyAsString(resp));
                }
                final HttpEntity entity = resp.getEntity();
                if (request instanceof HttpHead || entity == null) {
                    exchange.getIn().setBody(null);
                } else {
                    exchange.getIn().setBody(extractResponseBodyAsStream(entity.getContent(), exchange));
                }
                final String cType = (resp.getFirstHeader(CONTENT_TYPE) == null ? null :
                        resp.getFirstHeader(CONTENT_TYPE).getValue());
                exchange.getIn().setHeader(CONTENT_TYPE, cType);
                exchange.getIn().setHeader(HTTP_RESPONSE_CODE, resp.getStatusLine().getStatusCode());
            }
        } catch (final IOException e) {
            if (endpoint.getThrowExceptionOnFailure()) {
                throw e;
            }
        }
    }

    /**
     * Predicate to test for normal response codes.
     */
    private Predicate<CloseableHttpResponse> isValidResponse =
            response -> response.getStatusLine().getStatusCode() > 0 && response.getStatusLine().getStatusCode() < 300;

    /**
     * Get the response entity content as a string.
     * @param response the response object
     * @return the entity body or null
     */
    private String getEntityBodyAsString(final CloseableHttpResponse response) {
        final HttpEntity entity = response.getEntity();
        if (entity != null) {
            try (final InputStreamReader isr = new InputStreamReader(entity.getContent())) {
                return new BufferedReader(isr).lines().collect(Collectors.joining("\n"));
            } catch (final IOException e) {
                // Skip if we can't get the message
            }
        }
        return null;
    }

    /**
     * Build a Prefer header using any Prefer headers set up on the endpoint.
     * @return A prefer header string or null if none.
     */
    private String buildPreferHeader() {
        final List<URI> include = getPreferInclude(endpoint);
        final List<URI> omit = getPreferOmit(endpoint);
        if (!include.isEmpty() || !omit.isEmpty()) {
            final StringJoiner preferJoin = new StringJoiner("; ");
            preferJoin.add("return=representation");
            if (!include.isEmpty()) {
                final String tmpI = include.stream().map(URI::toString).collect(Collectors.joining(" "));
                if (tmpI.length() > 0) {
                    preferJoin.add("include=\"" + tmpI + "\"");
                }
            }
            if (!omit.isEmpty()) {
                final String tmpO = omit.stream().map(URI::toString).collect(Collectors.joining(" "));
                if (tmpO.length() > 0) {
                    preferJoin.add("omit=\"" + tmpO + "\"");
                }
            }
            return preferJoin.toString();
        }
        return null;
    }

    private URI getUri(final FcrepoEndpoint endpoint, final String url) throws IOException {
        if (endpoint.getFixity()) {
            return URI.create(url + FIXITY);
        } else if (endpoint.getMetadata()) {
            return getMetadataUri(url);
        }
        return URI.create(url);
    }

    private List<URI> getPreferOmit(final FcrepoEndpoint endpoint) {
        if (!isBlank(endpoint.getPreferOmit())) {
            return stream(endpoint.getPreferOmit().split("\\s+")).map(addPreferNamespace).map(URI::create)
                .collect(toList());
        }
        return emptyList();
    }

    private List<URI> getPreferInclude(final FcrepoEndpoint endpoint) {
        if (!isBlank(endpoint.getPreferInclude())) {
            return stream(endpoint.getPreferInclude().split("\\s+")).map(addPreferNamespace).map(URI::create)
                .collect(toList());
        }
        return emptyList();
    }

    /**
     * Retrieve the resource location from a HEAD request.
     */
    private URI getMetadataUri(final String url)
            throws IOException {
        final HttpHead head = new HttpHead(url);
        try (final CloseableHttpResponse headResponse = httpClient.execute(head, httpContext)) {
            if (headResponse.getFirstHeader(LOCATION) != null) {
                return URI.create(headResponse.getFirstHeader(LOCATION).getValue());
            }
            final List<URI> links = this.getLinkHeaders(headResponse, "describedby");
            if (links != null && links.size() == 1) {
                return links.get(0);
            } else {
                return URI.create(url);
            }
        }

    }

    /**
     * Get any link headers with the specified relationship
     * @param response the Http response
     * @param relationship the rel to match against
     * @return list of URIs
     */
    private List<URI> getLinkHeaders(final CloseableHttpResponse response, final String relationship) {
        return Stream.of(response.getHeaders("Link")).map(Header::getValue).map(FcrepoLink::new)
                .filter(link -> link.getRel().equalsIgnoreCase(relationship))
                .map(FcrepoLink::getUri).collect(Collectors.toList());
    }


    /**
     * Given an exchange, determine which HTTP method to use. Basically, use GET unless the value of the
     * Exchange.HTTP_METHOD header is defined. Unlike the http4: component, the request does not use POST if there is
     * a message body defined. This is so in order to avoid inadvertant changes to the repository.
     *
     * @param exchange the incoming message exchange
     */
    private String getMethod(final Exchange exchange) {
        final String method = exchange.getIn().getHeader(HTTP_METHOD, String.class);
        if (method == null) {
            return "GET";
        } else {
            return method.toUpperCase();
        }
    }

    /**
     * Given an exchange, extract the contentType value for use with a Content-Type header. The order of preference is
     * so: 1) a contentType value set on the endpoint 2) a contentType value set on the Exchange.CONTENT_TYPE header
     *
     * @param exchange the incoming message exchange
     */
    private String getContentType(final Exchange exchange) {
        final String contentTypeString = ExchangeHelper.getContentType(exchange);
        if (!isBlank(endpoint.getContentType())) {
            return endpoint.getContentType();
        } else if (!isBlank(contentTypeString)) {
            return contentTypeString;
        } else {
            return null;
        }
    }

    /**
     * Given an exchange, extract the value for use with an Accept header. The order of preference is:
     * 1) whether an accept value is set on the endpoint 2) a value set on
     * the Exchange.ACCEPT_CONTENT_TYPE header 3) a value set on an "Accept" header
     * 4) the endpoint DEFAULT_CONTENT_TYPE (i.e. application/rdf+xml)
     *
     * @param exchange the incoming message exchange
     */
    private String getAccept(final Exchange exchange) {
        final String acceptHeader = getAcceptHeader(exchange);
        if (!isBlank(endpoint.getAccept())) {
            return endpoint.getAccept();
        } else if (!isBlank(acceptHeader)) {
            return acceptHeader;
        } else if (!endpoint.getMetadata()) {
            return "*/*";
        } else {
            return DEFAULT_CONTENT_TYPE;
        }
    }

    /**
     * Given an exchange, extract the value of an incoming Accept header.
     *
     * @param exchange the incoming message exchange
     */
    private String getAcceptHeader(final Exchange exchange) {
        final Message in = exchange.getIn();
        if (!isBlank(in.getHeader("Accept", String.class))) {
            return in.getHeader("Accept", String.class);
        } else {
            return null;
        }
    }

    /**
     * Given an exchange, extract the fully qualified URL for a fedora resource. By default, this will use the entire
     * path set on the endpoint. If either of the following headers are defined, they will be appended to that path in
     * this order of preference: 1) FCREPO_URI 2) FCREPO_BASE_URL + FCREPO_IDENTIFIER
     *
     * @param exchange the incoming message exchange
     */
    private String getUrl(final Exchange exchange, final String transaction) {
        final String uri = exchange.getIn().getHeader(FCREPO_URI, "", String.class);
        if (!uri.isEmpty()) {
            return uri;
        }

        final String baseUrl = exchange.getIn().getHeader(FCREPO_BASE_URL, "", String.class);
        final StringBuilder url = new StringBuilder(baseUrl.isEmpty() ? endpoint.getBaseUrlWithScheme() : baseUrl);
        if (transaction != null) {
            url.append("/");
            url.append(transaction);
        }
        url.append(exchange.getIn().getHeader(FCREPO_IDENTIFIER, "", String.class));

        return url.toString();
    }

    private static Object extractResponseBodyAsStream(final InputStream is, final Exchange exchange) {
        // As httpclient is using a AutoCloseInputStream, it will be closed when the connection is closed
        // we need to cache the stream for it.
        if (is == null) {
            return null;
        }

        // convert the input stream to StreamCache if the stream cache is not disabled
        if (exchange.getProperty(DISABLE_HTTP_STREAM_CACHE, FALSE, Boolean.class)) {
            return is;
        } else {
            try (final CachedOutputStream cos = new CachedOutputStream(exchange)) {
                // This CachedOutputStream will not be closed when the exchange is onCompletion
                IOHelper.copyAndCloseInput(is, cos);
                // When the InputStream is closed, the CachedOutputStream will be closed
                return cos.newStreamCache();
            } catch (final IOException ex) {
                LOGGER.error("Error extracting body from http request", ex);
                return null;
            }
        }
    }
}
