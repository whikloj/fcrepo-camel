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

import static java.net.URI.create;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.camel.Exchange.CONTENT_TYPE;
import static org.apache.camel.Exchange.DISABLE_HTTP_STREAM_CACHE;
import static org.apache.camel.Exchange.HTTP_METHOD;
import static org.apache.camel.Exchange.HTTP_RESPONSE_CODE;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.LOCATION;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.apache.http.HttpStatus.SC_OK;
import static org.fcrepo.camel.FcrepoConstants.COMMIT;
import static org.fcrepo.camel.FcrepoConstants.ROLLBACK;
import static org.fcrepo.camel.FcrepoHeaders.FCREPO_IDENTIFIER;
import static org.fcrepo.camel.FcrepoHeaders.FCREPO_PREFER;
import static org.fcrepo.camel.FcrepoProducer.PREFER_PROPERTIES;
import static org.fcrepo.camel.TestUtils.N_TRIPLES;
import static org.fcrepo.camel.TestUtils.RDF_XML;
import static org.fcrepo.camel.TestUtils.TEXT_PLAIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fcrepo.client.FcrepoResponse;

import org.apache.camel.Exchange;
import org.apache.camel.ExtendedExchange;
import org.apache.camel.component.http.HttpMethods;
import org.apache.camel.converter.stream.InputStreamCache;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.engine.DefaultUnitOfWork;
import org.apache.camel.support.DefaultExchange;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.transaction.TransactionSystemException;

/**
 * @author acoburn
 * @author whikloj
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class FcrepoProducerTest {

    private static final String REPOSITORY = "http://fedora.info/definitions/v4/repository#";

    private static final String LDP = "http://www.w3.org/ns/ldp#";

    private static final String FEDORA_API = "http://fedora.info/definitions/fcrepo#";

    private FcrepoEndpoint testEndpoint;

    private FcrepoProducer testProducer;

    private ExtendedExchange testExchange;

    @Mock
    private CloseableHttpClient mockHttpClient;

    @Mock
    private CloseableHttpResponse mockGetResponse, mockHeadResponse, mockDeleteResponse, mockPostResponse,
            mockPatchResponse, mockPutResponse;

    @Mock
    private HttpEntity mockEntity1, mockEntity2;

    @Mock
    private StatusLine mockStatusLine1, mockStatusLine2;

    @Mock
    private Header mockHeader, mockNtripleHeader, mockRdfXmlHeader;

    @Captor
    private ArgumentCaptor<HttpRequestBase> requestArgumentCaptor;

    private Header[] emptyHeaders = new Header[]{};

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        final FcrepoComponent mockComponent = mock(FcrepoComponent.class);

        final FcrepoConfiguration testConfig = new FcrepoConfiguration();
        testEndpoint = new FcrepoEndpoint("fcrepo:localhost:8080", "/rest", mockComponent, testConfig);
        testEndpoint.setBaseUrl("localhost:8080/rest");
        testExchange = new DefaultExchange(new DefaultCamelContext());
        testExchange.getIn().setBody(null);

        when(mockEntity1.getContent()).thenReturn(null);
        when(mockNtripleHeader.getValue()).thenReturn(N_TRIPLES);
        when(mockRdfXmlHeader.getValue()).thenReturn(RDF_XML);
    }

    private void setUpHeadMocks() throws IOException {
        when(mockHttpClient.execute(any(HttpHead.class), any(HttpClientContext.class))).thenReturn(mockHeadResponse);
        when(mockHeadResponse.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockHeadResponse.getHeaders("Link")).thenReturn(emptyHeaders);
    }

    private void setUpGetMocks() throws IOException {
        when(mockHttpClient.execute(any(HttpGet.class), any(HttpClientContext.class))).thenReturn(mockGetResponse);
        when(mockGetResponse.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockGetResponse.getEntity()).thenReturn(mockEntity1);
        when(mockGetResponse.getHeaders("Link")).thenReturn(emptyHeaders);
    }

    private void setUpDeleteMocks() throws IOException {
        when(mockHttpClient.execute(any(HttpDelete.class), any(HttpClientContext.class))).thenReturn(
                mockDeleteResponse);
        when(mockDeleteResponse.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockDeleteResponse.getEntity()).thenReturn(mockEntity1);
    }

    private void setUpPostMocks() throws IOException {
        when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientContext.class))).thenReturn(mockPostResponse);
        when(mockPostResponse.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockPostResponse.getEntity()).thenReturn(mockEntity1);
    }

    private void setUpPatchMocks() throws IOException {
        when(mockHttpClient.execute(any(HttpPatch.class), any(HttpClientContext.class))).thenReturn(mockPatchResponse);
        when(mockPatchResponse.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockPatchResponse.getHeaders("Link")).thenReturn(null);
    }

    private void setUpPutMocks() throws IOException {
        when(mockHttpClient.execute(any(HttpPut.class), any(HttpClientContext.class))).thenReturn(mockPutResponse);
        when(mockPutResponse.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockPutResponse.getHeaders("Link")).thenReturn(null);
    }

    public void init() throws IOException {
        testProducer = new FcrepoProducer(testEndpoint);
        TestUtils.setField(testProducer, "httpClient", mockHttpClient);
    }

    @Test
    public void testGetProducer() throws Exception {
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfXml.getBytes());

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockRdfXmlHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfXml);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), TestUtils.RDF_XML);
        assertEquals(testExchange.getIn().getHeader(HTTP_RESPONSE_CODE), SC_OK);
    }

    @Test
    public void testGetAcceptHeaderProducer() throws Exception {
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfTriples.getBytes());

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");
        testExchange.getIn().setHeader("Accept", N_TRIPLES);

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockNtripleHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfTriples);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), N_TRIPLES);
    }

    @Test
    public void testGetPreferHeaderProducer() throws Exception {
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfTriples.getBytes());
        final String prefer = "return=representation; omit=\"http://www.w3.org/ns/ldp#PreferContainment\";";

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");
        testExchange.getIn().setHeader(FCREPO_PREFER, prefer);

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockNtripleHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfTriples);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), N_TRIPLES);
    }

    @Test
    public void testGetFixity() throws Exception {
        final String path = "/binary";
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.fixityTriples.getBytes());

        testEndpoint.setFixity(true);

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, path);

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockNtripleHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.fixityTriples);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), N_TRIPLES);
    }

    @Test
    public void testGetPreferIncludeLongEndpointProducer() throws Exception {
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfTriples.getBytes());

        testEndpoint.setPreferInclude("http://fedora.info/definitions/v4/repository#ServerManaged");

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockNtripleHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfTriples);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), N_TRIPLES);
    }

    @Test
    public void testGetPreferIncludeShortEndpointProducer() throws Exception {
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfTriples.getBytes());

        testEndpoint.setPreferInclude("PreferMembership");

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockNtripleHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfTriples);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), N_TRIPLES);
    }

    @Test
    public void testGetPreferOmitLongEndpointProducer() throws Exception {
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfTriples.getBytes());
        final String embed = "http://www.w3.org/ns/oa#PreferContainedDescriptions";

        testEndpoint.setPreferOmit(embed);

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockNtripleHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfTriples);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), N_TRIPLES);
    }

    @Test
    public void testGetPreferOmitShortEndpointProducer() throws Exception {
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfTriples.getBytes());

        testEndpoint.setPreferOmit("PreferContainment");

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockNtripleHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfTriples);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), N_TRIPLES);
    }

    @Test
    public void testGetAcceptEndpointProducer() throws Exception {
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfTriples.getBytes());

        testEndpoint.setAccept(N_TRIPLES);

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockNtripleHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfTriples);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), N_TRIPLES);
    }

    @Test
    public void testGetRootProducer() throws Exception {
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfXml.getBytes());

        init();

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockRdfXmlHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfXml);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), TestUtils.RDF_XML);
    }

    @Test
    public void testGetBinaryProducer() throws Exception {
        final String content = "Foo";
        final ByteArrayInputStream body = new ByteArrayInputStream(content.getBytes());

        testEndpoint.setMetadata(false);

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");
        testExchange.getIn().setHeader(ACCEPT, TestUtils.TEXT_PLAIN);
        testExchange.getIn().setHeader(HTTP_METHOD, HttpMethods.GET);

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeader.getValue()).thenReturn(TEXT_PLAIN);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), content);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), TestUtils.TEXT_PLAIN);
    }

    @Test
    public void testHeadProducer() throws Exception {
        final Map<String, List<String>> headers = new HashMap<>();
        headers.put(CONTENT_TYPE, singletonList(N_TRIPLES));
        headers.put("Link", singletonList("<" + TestUtils.baseUrl + "/bar>; rel=\"describedby\""));
        final int status = SC_OK;

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");
        testExchange.getIn().setHeader(HTTP_METHOD, HttpMethods.HEAD);

        setUpHeadMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockHeadResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockNtripleHeader);

        testProducer.process(testExchange);

        assertNull(testExchange.getIn().getBody());
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), N_TRIPLES);
        assertEquals(testExchange.getIn().getHeader(HTTP_RESPONSE_CODE), status);
    }

    @Test
    public void testDeleteProducer() throws Exception {
        final URI uri = create(TestUtils.baseUrl);
        final int status = SC_NO_CONTENT;
        final FcrepoResponse deleteResponse = new FcrepoResponse(uri, status, emptyMap(), null);

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");
        testExchange.getIn().setHeader(HTTP_METHOD, HttpMethods.DELETE);

        setUpDeleteMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(status);
        when(mockDeleteResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(null);

        testProducer.process(testExchange);

        assertNull(testExchange.getIn().getBody());
        assertNull(testExchange.getIn().getHeader(CONTENT_TYPE));
        assertEquals(testExchange.getIn().getHeader(HTTP_RESPONSE_CODE), status);
    }

    @Test
    public void testPostProducer() throws Exception {
        final String responseText = TestUtils.baseUrl + "/e8/0b/ab/e80bab60";
        final ByteArrayInputStream body = new ByteArrayInputStream(responseText.getBytes());
        final int status = SC_CREATED;

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");
        testExchange.getIn().setHeader(HTTP_METHOD, HttpMethods.POST);

        setUpHeadMocks();
        setUpPostMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(status);
        when(mockPostResponse.getEntity()).thenReturn(mockEntity1);
        when(mockEntity1.getContent()).thenReturn(body);
        when(mockPostResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockHeader);
        when(mockHeader.getValue()).thenReturn(TEXT_PLAIN);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), responseText);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE), TestUtils.TEXT_PLAIN);
        assertEquals(testExchange.getIn().getHeader(HTTP_RESPONSE_CODE), status);
    }

    @Test
    public void testPostContentTypeEndpointProducer() throws Exception {
        final int status = SC_NO_CONTENT;

        testEndpoint.setContentType(TestUtils.SPARQL_UPDATE);

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");
        testExchange.getIn().setHeader(HTTP_METHOD, HttpMethods.POST);
        testExchange.getIn().setBody(new ByteArrayInputStream(TestUtils.sparqlUpdate.getBytes()));

        setUpHeadMocks();
        setUpPostMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockStatusLine2.getStatusCode()).thenReturn(status);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockPostResponse.getStatusLine()).thenReturn(mockStatusLine2);
        when(mockEntity1.getContent()).thenReturn(null);

        testProducer.process(testExchange);

        assertNull(testExchange.getIn().getBody());
        assertEquals(testExchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE), status);
    }

    @Test
    public void testPatchProducer() throws Exception {
        final String metadata = "<" + TestUtils.baseUrl + "/bar>; rel=\"describedby\"";
        final int status = SC_NO_CONTENT;

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");
        testExchange.getIn().setHeader(HTTP_METHOD, HttpMethods.PATCH);
        testExchange.getIn().setBody(new ByteArrayInputStream(TestUtils.sparqlUpdate.getBytes()));

        setUpHeadMocks();
        setUpPatchMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockStatusLine2.getStatusCode()).thenReturn(status);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockHeadResponse.getHeaders("Link")).thenReturn(new Header[]{mockHeader});
        when(mockHeader.getValue()).thenReturn(metadata);
        when(mockPatchResponse.getStatusLine()).thenReturn(mockStatusLine2);
        when(mockPatchResponse.getEntity()).thenReturn(mockEntity1);
        when(mockEntity1.getContent()).thenReturn(null);

        testProducer.process(testExchange);

        verify(mockHttpClient, times(2)).execute(requestArgumentCaptor.capture(), any(HttpClientContext.class));
        final List<HttpRequestBase> requestArgs = requestArgumentCaptor.getAllValues();
        for (final HttpRequestBase req : requestArgs) {
            if (req.getMethod().equalsIgnoreCase("HEAD")) {
                assertEquals(TestUtils.baseUrl, req.getURI().toString());
            } else {
                assertEquals(TestUtils.baseUrl + "/bar", req.getURI().toString());
            }
        }
        assertNull(testExchange.getIn().getBody());
        assertEquals(testExchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE), status);
    }

    @Test
    public void testPutProducer() throws Exception {
        final int status = SC_CREATED;

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");
        testExchange.getIn().setHeader(HTTP_METHOD, HttpMethods.PUT);
        testExchange.getIn().setBody(null);

        setUpPutMocks();
        when(mockPutResponse.getEntity()).thenReturn(mockEntity1);
        when(mockEntity1.getContent()).thenReturn(new ByteArrayInputStream(TestUtils.baseUrl.getBytes()));
        when(mockPutResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockHeader);
        when(mockHeader.getValue()).thenReturn(TEXT_PLAIN);
        when(mockStatusLine1.getStatusCode()).thenReturn(status);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.baseUrl);
        assertEquals(testExchange.getIn().getHeader(HTTP_RESPONSE_CODE), status);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE), TestUtils.TEXT_PLAIN);
    }

    @Test
    public void testPreferProperties() throws Exception {
        testProducer = new FcrepoProducer(testEndpoint);

        assertEquals(6, PREFER_PROPERTIES.size());
        assertEquals(REPOSITORY + "ServerManaged", PREFER_PROPERTIES.get("ServerManaged"));
        assertEquals(FEDORA_API + "InboundReferences", PREFER_PROPERTIES.get("InboundReferences"));
        assertEquals("http://www.w3.org/ns/oa#PreferContainedDescriptions", PREFER_PROPERTIES.get("EmbedResources"));

        final String[] ldpPrefer = new String[] { "PreferContainment", "PreferMembership",
            "PreferMinimalContainer" };
        for (final String s : ldpPrefer) {
            assertEquals(LDP + s, PREFER_PROPERTIES.get(s));
        }
    }

    @Test
    public void testGetProducerWithScheme() throws Exception {
        final int status = SC_OK;
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfXml.getBytes());

        // set the baseUrl with an explicit http:// scheme
        testEndpoint.setBaseUrl("http://localhost:8080/rest");
        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getStatusLine()).thenReturn(mockStatusLine2);
        when(mockStatusLine2.getStatusCode()).thenReturn(SC_OK);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockRdfXmlHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfXml);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), TestUtils.RDF_XML);
        assertEquals(testExchange.getIn().getHeader(HTTP_RESPONSE_CODE), status);
    }

    @Test
    public void testGetSecureProducer() throws Exception {
        final int status = 200;
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfXml.getBytes());

        // set the baseUrl with no scheme but with a secure port
        testEndpoint.setBaseUrl("localhost:443/rest");
        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/secure");

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getStatusLine()).thenReturn(mockStatusLine2);
        when(mockStatusLine2.getStatusCode()).thenReturn(SC_OK);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockRdfXmlHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfXml);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), TestUtils.RDF_XML);
        assertEquals(testExchange.getIn().getHeader(HTTP_RESPONSE_CODE), status);
    }

    @Test
    public void testGetSecureProducerWithScheme() throws Exception {
        final int status = 200;
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfXml.getBytes());

        // set the baseUrl with explicit scheme but no port
        testEndpoint.setBaseUrl("https://localhost/rest");
        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/secure");

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockGetResponse.getStatusLine()).thenReturn(mockStatusLine2);
        when(mockStatusLine2.getStatusCode()).thenReturn(SC_OK);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockRdfXmlHeader);
        when(mockEntity1.getContent()).thenReturn(body);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfXml);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), TestUtils.RDF_XML);
        assertEquals(testExchange.getIn().getHeader(HTTP_RESPONSE_CODE), status);
    }

    @Test
    public void testTransactedGetProducer() throws Exception {
        final String baseUrl = "http://localhost:8080/rest";
        final String path = "/transact";
        final String path2 = "/transact2";
        final String tx = "tx:12345";
        final String txUrl = baseUrl + "/" + tx;
        final URI uri = create(baseUrl + "/" + tx + path);
        final URI uri2 = create(baseUrl + "/" + tx + path2);
        final URI beginUri = URI.create(baseUrl + FcrepoConstants.TRANSACTION);
        final URI commitUri = URI.create(txUrl + COMMIT);
        final int status = 200;
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfXml.getBytes());
        final ByteArrayInputStream body2 = new ByteArrayInputStream(TestUtils.rdfTriples.getBytes());
        final DefaultUnitOfWork uow = new DefaultUnitOfWork(testExchange);
        final FcrepoTransactionManager txMgr = new FcrepoTransactionManager();
        txMgr.setBaseUrl(baseUrl);

        testEndpoint.setTransactionManager(txMgr);

        final CloseableHttpResponse mockTxBeginResponse = mock(CloseableHttpResponse.class);
        final Header mockTxHeader = mock(Header.class);
        final StatusLine beginTxStatusLine = mock(StatusLine.class);
        when(mockTxBeginResponse.getFirstHeader(LOCATION)).thenReturn(mockTxHeader);
        when(mockTxBeginResponse.getStatusLine()).thenReturn(beginTxStatusLine);
        when(beginTxStatusLine.getStatusCode()).thenReturn(SC_CREATED);
        when(mockTxHeader.getValue()).thenReturn(txUrl);

        final CloseableHttpResponse mockTxCommitResponse = mock(CloseableHttpResponse.class);
        final StatusLine commitTxStatusLine = mock(StatusLine.class);
        when(mockTxCommitResponse.getStatusLine()).thenReturn(commitTxStatusLine);
        when(commitTxStatusLine.getStatusCode()).thenReturn(SC_NO_CONTENT);

        init();
        TestUtils.setField(txMgr, "httpClient", mockHttpClient);

        uow.beginTransactedBy((Object)tx);

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, path);

        testExchange.setUnitOfWork(uow);

        final CloseableHttpResponse mockHead2Response = mock(CloseableHttpResponse.class);
        final CloseableHttpResponse mockGet2Response = mock(CloseableHttpResponse.class);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockHead2Response.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockHeadResponse.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockGetResponse.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockHead2Response.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockGet2Response.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);

        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockRdfXmlHeader);
        when(mockGetResponse.getEntity()).thenReturn(mockEntity1);
        when(mockEntity1.getContent()).thenReturn(body);
        when(mockGet2Response.getFirstHeader(CONTENT_TYPE)).thenReturn(mockNtripleHeader);
        when(mockGet2Response.getEntity()).thenReturn(mockEntity2);
        when(mockEntity2.getContent()).thenReturn(body2);
        // None of these objects are binaries so there are no "describedby" link headers.
        when(mockHeadResponse.getHeaders("Link")).thenReturn(emptyHeaders);
        when(mockHead2Response.getHeaders("Link")).thenReturn(emptyHeaders);
        when(mockGetResponse.getHeaders("Link")).thenReturn(emptyHeaders);
        when(mockGet2Response.getHeaders("Link")).thenReturn(emptyHeaders);

        when(mockHttpClient.execute(any(HttpRequestBase.class), any(HttpClientContext.class))).thenAnswer(I -> {
                final HttpRequestBase req = I.getArgument(0);
                if (req.getMethod().equalsIgnoreCase("head")) {
                    if (req.getURI().equals(uri)) {
                        return mockHeadResponse;
                    } else if (req.getURI().equals(uri2)) {
                        return mockHead2Response;
                    }
                } else if (req.getMethod().equalsIgnoreCase("get")) {
                    if (req.getURI().equals(uri)) {
                        return mockGetResponse;
                    } else if (req.getURI().equals(uri2)) {
                        return mockGet2Response;
                    }
                } else if (req.getMethod().equalsIgnoreCase("post")) {
                    if (req.getURI().equals(beginUri)) {
                        return mockTxBeginResponse;
                    } else if (req.getURI().equals(commitUri)) {
                        return mockTxCommitResponse;
                    }
                }
                return null;
        });

        testProducer.process(testExchange);

        assertEquals(status, testExchange.getIn().getHeader(HTTP_RESPONSE_CODE));
        assertEquals(TestUtils.RDF_XML, testExchange.getIn().getHeader(CONTENT_TYPE, String.class));
        assertEquals(TestUtils.rdfXml, testExchange.getIn().getBody(String.class));

        testExchange.getIn().setHeader(HTTP_METHOD, "GET");
        testExchange.getIn().setHeader(ACCEPT, N_TRIPLES);
        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, path2);
        testExchange.setUnitOfWork(uow);
        testProducer.process(testExchange);
        assertEquals(status, testExchange.getIn().getHeader(HTTP_RESPONSE_CODE));
        assertEquals(N_TRIPLES, testExchange.getIn().getHeader(CONTENT_TYPE, String.class));
        assertEquals(TestUtils.rdfTriples, testExchange.getIn().getBody(String.class));
    }

    @Test(expected = TransactionSystemException.class)
    public void testTransactedProducerWithError() throws Exception {
        final String baseUrl = "http://localhost:8080/rest";
        final String path = "/transact";
        final String path2 = "/transact2";
        final String tx = "tx:12345";
        final String txUrl = baseUrl + "/" + tx;
        final URI uri = create(baseUrl + "/" + tx + path);
        final URI uri2 = create(baseUrl + "/" + tx + path2);
        final URI commitUri = URI.create(txUrl + COMMIT);
        final URI rollbackUri = URI.create(txUrl + ROLLBACK);
        final URI beginUri = URI.create(baseUrl + FcrepoConstants.TRANSACTION);
        final int status = 200;
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfXml.getBytes());
        final ByteArrayInputStream body2 = new ByteArrayInputStream(TestUtils.rdfTriples.getBytes());
        final DefaultUnitOfWork uow = new DefaultUnitOfWork(testExchange);
        final FcrepoTransactionManager txMgr = new FcrepoTransactionManager();
        txMgr.setBaseUrl(baseUrl);

        testEndpoint.setTransactionManager(txMgr);

        init();
        TestUtils.setField(txMgr, "httpClient", mockHttpClient);

        uow.beginTransactedBy((Object)tx);

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, path);
        testExchange.setUnitOfWork(uow);

        final CloseableHttpResponse mockTxBeginResponse = mock(CloseableHttpResponse.class);
        final Header mockTxHeader = mock(Header.class);
        final StatusLine beginTxStatusLine = mock(StatusLine.class);
        when(mockTxBeginResponse.getFirstHeader(LOCATION)).thenReturn(mockTxHeader);
        when(mockTxBeginResponse.getStatusLine()).thenReturn(beginTxStatusLine);
        when(beginTxStatusLine.getStatusCode()).thenReturn(SC_CREATED);
        when(mockTxHeader.getValue()).thenReturn(txUrl);

        final CloseableHttpResponse mockTxCommitResponse = mock(CloseableHttpResponse.class);
        final StatusLine commitTxStatusLine = mock(StatusLine.class);
        when(mockTxCommitResponse.getStatusLine()).thenReturn(commitTxStatusLine);
        when(commitTxStatusLine.getStatusCode()).thenReturn(SC_NO_CONTENT);

        final CloseableHttpResponse mockHead2Response = mock(CloseableHttpResponse.class);
        final CloseableHttpResponse mockGet2Response = mock(CloseableHttpResponse.class);
        when(mockHeadResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockHead2Response.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockHeadResponse.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockGetResponse.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockHead2Response.getStatusLine()).thenReturn(mockStatusLine1);
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);

        when(mockGet2Response.getStatusLine()).thenReturn(mockStatusLine2);
        when(mockStatusLine2.getStatusCode()).thenReturn(SC_BAD_REQUEST);

        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockRdfXmlHeader);
        when(mockGetResponse.getEntity()).thenReturn(mockEntity1);
        when(mockEntity1.getContent()).thenReturn(body);

        // None of these objects are binaries so there are no "describedby" link headers.
        when(mockHeadResponse.getHeaders("Link")).thenReturn(emptyHeaders);
        when(mockHead2Response.getHeaders("Link")).thenReturn(emptyHeaders);
        when(mockGetResponse.getHeaders("Link")).thenReturn(emptyHeaders);
        when(mockGet2Response.getHeaders("Link")).thenReturn(emptyHeaders);

        // When the request fails with the 400, the transaction is rolled back so a Delete operation is called.
        final StatusLine mockStatusLine3 = mock(StatusLine.class);
        when(mockDeleteResponse.getStatusLine()).thenReturn(mockStatusLine3);
        when(mockStatusLine3.getStatusCode()).thenReturn(SC_NO_CONTENT);

        when(mockHttpClient.execute(any(HttpRequestBase.class), any(HttpClientContext.class))).thenAnswer(I -> {
            final HttpRequestBase req = I.getArgument(0);
            if (req.getMethod().equalsIgnoreCase("head")) {
                if (req.getURI().equals(uri)) {
                    return mockHeadResponse;
                } else if (req.getURI().equals(uri2)) {
                    return mockHead2Response;
                }
            } else if (req.getMethod().equalsIgnoreCase("get")) {
                if (req.getURI().equals(uri)) {
                    return mockGetResponse;
                } else if (req.getURI().equals(uri2)) {
                    return mockGet2Response;
                }
            } else if (req.getMethod().equalsIgnoreCase("post")) {
                if (req.getURI().equals(beginUri)) {
                    return mockTxBeginResponse;
                } else if (req.getURI().equals(commitUri)) {
                    return mockTxCommitResponse;
                } else if (req.getURI().equals(rollbackUri)) {
                    return mockDeleteResponse;
                }
            }
            return null;
        });

        testProducer.process(testExchange);

        assertEquals(status, testExchange.getIn().getHeader(HTTP_RESPONSE_CODE));
        assertEquals(TestUtils.RDF_XML, testExchange.getIn().getHeader(CONTENT_TYPE, String.class));
        assertEquals(TestUtils.rdfXml, testExchange.getIn().getBody(String.class));

        testExchange.getIn().setHeader(HTTP_METHOD, "GET");
        testExchange.getIn().setHeader(ACCEPT, N_TRIPLES);
        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, path2);
        testExchange.setUnitOfWork(uow);
        testProducer.process(testExchange);

    }

    @Test
    public void testNoStreamCaching() throws Exception {
        final int status = 200;
        final ByteArrayInputStream body = new ByteArrayInputStream(TestUtils.rdfXml.getBytes());

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");
        testExchange.setProperty(DISABLE_HTTP_STREAM_CACHE, true);

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockEntity1.getContent()).thenReturn(body);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockRdfXmlHeader);

        testProducer.process(testExchange);

        assertEquals(testExchange.getIn().getBody(String.class), TestUtils.rdfXml);
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), TestUtils.RDF_XML);
        assertEquals(testExchange.getIn().getHeader(HTTP_RESPONSE_CODE), status);
    }

    @Test
    public void testStreamCaching() throws Exception {
        final int status = 200;
        final String rdfConcat = StringUtils.repeat(TestUtils.rdfXml, 10000);
        final ByteArrayInputStream body = new ByteArrayInputStream(rdfConcat.getBytes());

        init();

        testExchange.getIn().setHeader(FCREPO_IDENTIFIER, "/foo");
        testExchange.setProperty(DISABLE_HTTP_STREAM_CACHE, false);

        testExchange.getContext().getStreamCachingStrategy().setSpoolThreshold(1024);
        testExchange.getContext().getStreamCachingStrategy().setBufferSize(256);
        testExchange.getContext().setStreamCaching(true);

        setUpHeadMocks();
        setUpGetMocks();
        when(mockStatusLine1.getStatusCode()).thenReturn(SC_OK);
        when(mockEntity1.getContent()).thenReturn(body);
        when(mockGetResponse.getFirstHeader(CONTENT_TYPE)).thenReturn(mockRdfXmlHeader);

        testProducer.process(testExchange);

        assertEquals(true, testExchange.getContext().isStreamCaching());
        assertNotNull(testExchange.getIn().getBody(InputStreamCache.class));
        assertEquals(rdfConcat.length(), testExchange.getIn().getBody(InputStreamCache.class).length());
        assertEquals(rdfConcat.length(), testExchange.getIn().getBody(InputStreamCache.class).length());
        assertEquals(testExchange.getIn().getHeader(CONTENT_TYPE, String.class), TestUtils.RDF_XML);
        assertEquals(testExchange.getIn().getHeader(HTTP_RESPONSE_CODE), status);
    }
}
