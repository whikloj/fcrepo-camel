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
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.fcrepo.camel.FcrepoConstants.COMMIT;
import static org.fcrepo.camel.FcrepoConstants.ROLLBACK;
import static org.fcrepo.camel.FcrepoConstants.TRANSACTION;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * A Transaction Manager for interacting with fedora-based transactions
 *
 * @author Aaron Coburn
 * @since Feb 16, 2015
 */
public class FcrepoTransactionManager extends AbstractPlatformTransactionManager {

    private CloseableHttpClient httpClient;

    private HttpClientContext httpContext;

    private String baseUrl;

    private String authUsername;

    private String authPassword;

    private String authHost;

    private static final Logger LOGGER = getLogger(FcrepoTransactionManager.class);

    /**
     * Create a FcrepoTransactionManager
     */
    public FcrepoTransactionManager() {
        super();
        setNestedTransactionAllowed(false);
    }

    /**
     * Set the baseUrl for the transaction manager.
     *
     * @param baseUrl the fcrepo base url
     */
    public void setBaseUrl(final String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /**
     * Get the base url for the transaction manager.
     *
     * @return the fcrepo base url
     */
    public String getBaseUrl() {
        return baseUrl;
    }

    /**
     * Set the authUsername for the transaction manager.
     *
     * @param authUsername the username for authentication
     */
    public void setAuthUsername(final String authUsername) {
        this.authUsername = authUsername;
    }

    /**
     * Get the authUsername for the transaction manager.
     *
     * @return the username for authentication
     */
    public String getAuthUsername() {
        return authUsername;
    }

    /**
     * Set the authPassword for the transaction manager.
     *
     * @param authPassword the password used for authentication
     */
    public void setAuthPassword(final String authPassword) {
        this.authPassword = authPassword;
    }

    /**
     * Get the authPassword for the transaction manager.
     *
     * @return the password used for authentication
     */
    public String getAuthPassword() {
        return authPassword;
    }

    /**
     * Set the authHost for the transaction manager.
     *
     * @param authHost the host realm used for authentication
     */
    public void setAuthHost(final String authHost) {
        this.authHost = authHost;
    }

    /**
     * Get the authHost for the transaction manager.
     *
     * @return the host realm used for authentication
     */
    public String getAuthHost() {
        if (authHost != null) {
            return authHost;
        }
        return getAuthHostFromBaseUrl();
    }

    @Override
    protected void doBegin(final Object transaction, final TransactionDefinition definition) {
        final FcrepoTransactionObject tx = (FcrepoTransactionObject)transaction;

        if (tx.getSessionId() == null) {
            final HttpPost txPost = new HttpPost(baseUrl + TRANSACTION);
            try (final CloseableHttpResponse response = getClient().execute(txPost, getContext())) {
                if (response.getStatusLine().getStatusCode() == SC_CREATED &&
                        response.getFirstHeader(LOCATION) != null) {
                    final String location = response.getFirstHeader(LOCATION).getValue();
                    tx.setSessionId(location.substring(baseUrl.length() + 1));
                } else {
                    LOGGER.debug("Got bad response {}", response.getStatusLine().getStatusCode());
                    throw new CannotCreateTransactionException("Invalid response while creating transaction");
                }
            } catch (final IOException e) {
                throw new CannotCreateTransactionException("Invalid response while creating transaction");
            }
        }
    }

    @Override
    protected void doCommit(final DefaultTransactionStatus status) {
        final FcrepoTransactionObject tx = (FcrepoTransactionObject)status.getTransaction();
        final HttpPost txPut = new HttpPost(baseUrl + "/" + tx.getSessionId() + COMMIT);
        try (final CloseableHttpResponse response = getClient().execute(txPut, getContext())) {
            if (response.getStatusLine().getStatusCode() != SC_NO_CONTENT) {
                LOGGER.debug("Could not commit fcrepo transaction: {}",
                        debugTxError(response));
                throw new TransactionSystemException("Could not commit fcrepo transaction");
            }
        } catch (final IOException e) {
            LOGGER.debug("Transaction commit failed: ", e);
            throw new TransactionSystemException("Could not commit fcrepo transaction");
        } finally {
            tx.setSessionId(null);
        }
    }

    @Override
    protected void doRollback(final DefaultTransactionStatus status) {
        final FcrepoTransactionObject tx = (FcrepoTransactionObject)status.getTransaction();
        final HttpPost txDelete = new HttpPost(baseUrl + "/" + tx.getSessionId() + ROLLBACK);
        try (final CloseableHttpResponse response = getClient().execute(txDelete, getContext())) {
            if (response.getStatusLine().getStatusCode() != SC_NO_CONTENT) {
                LOGGER.debug("Could not rollback fcrepo transaction: {}",
                        debugTxError(response));
                throw new TransactionSystemException("Could not rollback fcrepo transaction");
            }
        } catch (final IOException e) {
            LOGGER.debug("Transaction rollback failed: ", e);
            throw new TransactionSystemException("Could not rollback fcrepo transaction");
        } finally {
            tx.setSessionId(null);
        }
    }

    private String debugTxError(final CloseableHttpResponse response) {
        final String message;
        final int responseCode = response.getStatusLine().getStatusCode();
        switch (responseCode) {
            case 404:
                message = "No transaction found with the provided ID.";
                break;
            case 410:
                message = "The transaction had already expired.";
                break;
            case 409:
                String messageBody = null;
                try (final InputStreamReader isr = new InputStreamReader(response.getEntity().getContent())) {
                    messageBody = new BufferedReader(isr).lines().collect(Collectors.joining("\n"));
                } catch (final IOException e) {
                    // Skip
                }
                message = "Error completing your request: " + (messageBody != null ? messageBody : "<message " +
                        "unavailable>");
                break;
            default:
                message = "Response code " + responseCode + " was completely unexpected.";
        }
        return message;
    }

    @Override
    protected Object doGetTransaction() {
        return new FcrepoTransactionObject();
    }

    private CloseableHttpClient getClient() {
        if (httpClient == null) {
            httpClient = getBuilder().buildClient();
        }
        return httpClient;
    }

    private HttpClientContext getContext() {
        if (httpContext == null) {
            httpContext = getBuilder().buildContext();
        }
        return httpContext;
    }

    private FcrepoHttpBuilder getBuilder() {
        return FcrepoHttpBuilder.create().setCredentials(getAuthUsername(), getAuthPassword())
                .setAuthHost(getAuthHost());
    }

    private String getAuthHostFromBaseUrl() {
        if (getBaseUrl() != null) {
            String noScheme = getBaseUrl().replaceAll("^https?://", "");
            while (noScheme.contains("/")) {
                noScheme = noScheme.substring(0, noScheme.lastIndexOf("/"));
            }
            return noScheme;
        }
        return null;
    }
}
