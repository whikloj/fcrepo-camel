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

import static org.apache.http.HttpHeaders.LOCATION;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.fcrepo.camel.FcrepoConstants.COMMIT;
import static org.fcrepo.camel.FcrepoConstants.ROLLBACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;

import org.apache.http.Header;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @author Aaron Coburn
 * @author whikloj
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class FcrepoTransactionManagerTest {

    @Mock
    private CloseableHttpResponse mockPostResponse, mockPostResponse2, mockDeleteResponse;

    @Mock
    private CloseableHttpClient mockHttpClient;

    @Mock
    private Header mockLocationHeader;

    @Mock
    private StatusLine mockCreatedStatus, mockNoContentStatus, mockBadRequestStatus;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        when(mockCreatedStatus.getStatusCode()).thenReturn(SC_CREATED);
        when(mockNoContentStatus.getStatusCode()).thenReturn(SC_NO_CONTENT);
        when(mockBadRequestStatus.getStatusCode()).thenReturn(SC_BAD_REQUEST);
    }

    @Test
    public void testProperties() {
        final FcrepoTransactionManager txMgr = new FcrepoTransactionManager();
        final String baseUrl = "http://localhost:8080/rest";
        final String authUsername = "foo";
        final String authPassword = "bar";
        final String authHost = "baz";

        assertNull(txMgr.getAuthUsername());
        assertNull(txMgr.getAuthPassword());
        assertNull(txMgr.getBaseUrl());
        assertNull(txMgr.getAuthHost());

        txMgr.setBaseUrl(baseUrl);
        txMgr.setAuthUsername(authUsername);
        txMgr.setAuthPassword(authPassword);
        txMgr.setAuthHost(authHost);

        assertEquals(baseUrl, txMgr.getBaseUrl());
        assertEquals(authUsername, txMgr.getAuthUsername());
        assertEquals(authPassword, txMgr.getAuthPassword());
        assertEquals(authHost, txMgr.getAuthHost());
    }

    @Test
    public void testTransactionCommit() throws IOException, TransactionSystemException {
        final String baseUrl = "http://localhost:8080/rest";
        final String tx = "tx:1234567890";
        final String txUrl = baseUrl + "/" + tx;
        final URI commitUri = URI.create(txUrl + COMMIT);
        final URI beginUri = URI.create(baseUrl + FcrepoConstants.TRANSACTION);
        final FcrepoTransactionManager txMgr = new FcrepoTransactionManager();
        txMgr.setBaseUrl(baseUrl);
        TestUtils.setField(txMgr, "httpClient", mockHttpClient);

        final TransactionTemplate transactionTemplate = new TransactionTemplate(txMgr);
        final DefaultTransactionDefinition txDef = new DefaultTransactionDefinition(
                TransactionDefinition.PROPAGATION_REQUIRED);

        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.afterPropertiesSet();

        when(mockPostResponse.getFirstHeader(LOCATION)).thenReturn(mockLocationHeader);
        when(mockLocationHeader.getValue()).thenReturn(txUrl);
        when(mockPostResponse.getStatusLine()).thenReturn(mockCreatedStatus);
        when(mockPostResponse.getEntity()).thenReturn(null);
        when(mockPostResponse2.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockPostResponse2.getStatusLine()).thenReturn(mockNoContentStatus);
        when(mockPostResponse2.getEntity()).thenReturn(null);
        when(mockHttpClient.execute(any(HttpRequestBase.class), any(HttpClientContext.class))).thenAnswer(a -> {
            final HttpRequestBase req = a.getArgument(0);
            if (req.getURI().equals(beginUri)) {
                return mockPostResponse;
            } else if (req.getURI().equals(commitUri)) {
                return mockPostResponse2;
            }
            return null;
        });

        DefaultTransactionStatus status = (DefaultTransactionStatus)txMgr.getTransaction(txDef);
        FcrepoTransactionObject txObj = (FcrepoTransactionObject)status.getTransaction();

        assertEquals(tx, txObj.getSessionId());
        assertFalse(status.isCompleted());

        status = (DefaultTransactionStatus)txMgr.getTransaction(txDef);

        txMgr.commit(status);

        txObj = (FcrepoTransactionObject)status.getTransaction();

        assertNull(txObj.getSessionId());
        assertTrue(status.isCompleted());
    }

    @Test
    public void testTransactionRollback() throws IOException {
        final String baseUrl = "http://localhost:8080/rest";
        final String tx = "tx:1234567890";
        final String txUrl = baseUrl + "/" + tx;
        final URI commitUri = URI.create(txUrl + COMMIT);
        final URI rollbackUri = URI.create(txUrl + ROLLBACK);
        final URI beginUri = URI.create(baseUrl + FcrepoConstants.TRANSACTION);
        final FcrepoTransactionManager txMgr = new FcrepoTransactionManager();
        txMgr.setBaseUrl(baseUrl);
        TestUtils.setField(txMgr, "httpClient", mockHttpClient);

        final TransactionTemplate transactionTemplate = new TransactionTemplate(txMgr);
        final DefaultTransactionDefinition txDef = new DefaultTransactionDefinition(
                TransactionDefinition.PROPAGATION_REQUIRED);

        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.afterPropertiesSet();

        when(mockPostResponse.getFirstHeader(LOCATION)).thenReturn(mockLocationHeader);
        when(mockLocationHeader.getValue()).thenReturn(txUrl);
        when(mockPostResponse.getStatusLine()).thenReturn(mockCreatedStatus);
        when(mockPostResponse.getEntity()).thenReturn(null);
        when(mockPostResponse2.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockPostResponse2.getStatusLine()).thenReturn(mockNoContentStatus);
        when(mockPostResponse2.getEntity()).thenReturn(null);
        when(mockDeleteResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockDeleteResponse.getStatusLine()).thenReturn(mockNoContentStatus);
        when(mockHttpClient.execute(any(HttpRequestBase.class), any(HttpClientContext.class))).thenAnswer(a -> {
            final HttpRequestBase req = a.getArgument(0);
            if (req.getURI().equals(beginUri)) {
                return mockPostResponse;
            } else if (req.getURI().equals(commitUri)) {
                return mockPostResponse2;
            } else if (req.getURI().equals(rollbackUri)) {
                return mockDeleteResponse;
            }
            return null;
        });

        DefaultTransactionStatus status = (DefaultTransactionStatus)txMgr.getTransaction(txDef);
        FcrepoTransactionObject txObj = (FcrepoTransactionObject)status.getTransaction();

        assertEquals(tx, txObj.getSessionId());
        assertFalse(status.isCompleted());

        status = (DefaultTransactionStatus)txMgr.getTransaction(txDef);

        txMgr.rollback(status);

        txObj = (FcrepoTransactionObject)status.getTransaction();

        assertNull(txObj.getSessionId());
        assertTrue(status.isCompleted());
    }

    @Test (expected = CannotCreateTransactionException.class)
    public void testTransactionBeginError() throws IOException {
        final String baseUrl = "http://localhost:8080/rest";
        final FcrepoTransactionManager txMgr = new FcrepoTransactionManager();
        txMgr.setBaseUrl(baseUrl);
        TestUtils.setField(txMgr, "httpClient", mockHttpClient);

        final TransactionTemplate transactionTemplate = new TransactionTemplate(txMgr);
        final DefaultTransactionDefinition txDef = new DefaultTransactionDefinition(
                TransactionDefinition.PROPAGATION_REQUIRED);

        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.afterPropertiesSet();

        when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientContext.class))).thenReturn(mockPostResponse);
        when(mockPostResponse.getStatusLine()).thenReturn(mockBadRequestStatus);
        when(mockPostResponse.getFirstHeader(LOCATION)).thenReturn(null);

        txMgr.getTransaction(txDef);
    }

    @Test (expected = CannotCreateTransactionException.class)
    public void testTransactionBeginNoLocationError() throws IOException {
        final String baseUrl = "http://localhost:8080/rest";
        final FcrepoTransactionManager txMgr = new FcrepoTransactionManager();
        txMgr.setBaseUrl(baseUrl);
        TestUtils.setField(txMgr, "httpClient", mockHttpClient);

        final TransactionTemplate transactionTemplate = new TransactionTemplate(txMgr);
        final DefaultTransactionDefinition txDef = new DefaultTransactionDefinition(
                TransactionDefinition.PROPAGATION_REQUIRED);

        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.afterPropertiesSet();

        when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientContext.class))).thenReturn(mockPostResponse);
        when(mockPostResponse.getStatusLine()).thenReturn(mockCreatedStatus);
        when(mockPostResponse.getFirstHeader(LOCATION)).thenReturn(null);

        txMgr.getTransaction(txDef);
    }

    @Test (expected = TransactionSystemException.class)
    public void testTransactionCommitError() throws IOException {
        final String baseUrl = "http://localhost:8080/rest";
        final String tx = "tx:1234567890";
        final String txUrl = baseUrl + "/" + tx;
        final URI commitUri = URI.create(txUrl + COMMIT);
        final URI beginUri = URI.create(baseUrl + FcrepoConstants.TRANSACTION);
        final FcrepoTransactionManager txMgr = new FcrepoTransactionManager();
        txMgr.setBaseUrl(baseUrl);
        TestUtils.setField(txMgr, "httpClient", mockHttpClient);

        final TransactionTemplate transactionTemplate = new TransactionTemplate(txMgr);
        final DefaultTransactionDefinition txDef = new DefaultTransactionDefinition(
                TransactionDefinition.PROPAGATION_REQUIRED);

        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.afterPropertiesSet();

        when(mockPostResponse.getFirstHeader(LOCATION)).thenReturn(mockLocationHeader);
        when(mockLocationHeader.getValue()).thenReturn(txUrl);
        when(mockPostResponse.getStatusLine()).thenReturn(mockCreatedStatus);
        when(mockPostResponse.getEntity()).thenReturn(null);
        when(mockPostResponse2.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockPostResponse2.getStatusLine()).thenReturn(mockBadRequestStatus);
        when(mockPostResponse2.getEntity()).thenReturn(null);
        when(mockHttpClient.execute(any(HttpRequestBase.class), any(HttpClientContext.class))).thenAnswer(a -> {
            final HttpRequestBase req = a.getArgument(0);
            if (req.getURI().equals(beginUri)) {
                return mockPostResponse;
            } else if (req.getURI().equals(commitUri)) {
                return mockPostResponse2;
            }
            return null;
        });

        DefaultTransactionStatus status = (DefaultTransactionStatus)txMgr.getTransaction(txDef);

        final FcrepoTransactionObject txObj = (FcrepoTransactionObject)status.getTransaction();
        assertEquals(tx, txObj.getSessionId());
        assertFalse(status.isCompleted());

        status = (DefaultTransactionStatus)txMgr.getTransaction(txDef);
        txMgr.commit(status);
    }

    @Test (expected = TransactionSystemException.class)
    public void testTransactionRollbackError() throws IOException {
        final String baseUrl = "http://localhost:8080/rest";
        final String tx = "tx:1234567890";
        final String txUrl = baseUrl + "/" + tx;
        final URI rollbackUri = URI.create(txUrl + FcrepoConstants.ROLLBACK);
        final URI beginUri = URI.create(baseUrl + FcrepoConstants.TRANSACTION);
        final FcrepoTransactionManager txMgr = new FcrepoTransactionManager();
        txMgr.setBaseUrl(baseUrl);
        TestUtils.setField(txMgr, "httpClient", mockHttpClient);

        final TransactionTemplate transactionTemplate = new TransactionTemplate(txMgr);
        final DefaultTransactionDefinition txDef = new DefaultTransactionDefinition(
                TransactionDefinition.PROPAGATION_REQUIRED);

        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.afterPropertiesSet();

        when(mockPostResponse.getFirstHeader(LOCATION)).thenReturn(mockLocationHeader);
        when(mockLocationHeader.getValue()).thenReturn(txUrl);
        when(mockPostResponse.getStatusLine()).thenReturn(mockCreatedStatus);
        when(mockPostResponse.getEntity()).thenReturn(null);
        when(mockDeleteResponse.getFirstHeader(LOCATION)).thenReturn(null);
        when(mockDeleteResponse.getStatusLine()).thenReturn(mockBadRequestStatus);
        when(mockDeleteResponse.getEntity()).thenReturn(null);
        when(mockHttpClient.execute(any(HttpRequestBase.class), any(HttpClientContext.class))).thenAnswer(a -> {
            final HttpRequestBase req = a.getArgument(0);
            if (req.getURI().equals(beginUri)) {
                return mockPostResponse;
            } else if (req.getURI().equals(rollbackUri)) {
                return mockDeleteResponse;
            }
            return null;
        });

        DefaultTransactionStatus status = (DefaultTransactionStatus)txMgr.getTransaction(txDef);

        final FcrepoTransactionObject txObj = (FcrepoTransactionObject)status.getTransaction();
        assertEquals(tx, txObj.getSessionId());
        assertFalse(status.isCompleted());

        status = (DefaultTransactionStatus)txMgr.getTransaction(txDef);
        txMgr.rollback(status);
    }
}
