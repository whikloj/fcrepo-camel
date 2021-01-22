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

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

/**
 * Build http clients/context.
 * @author whikloj
 */
public class FcrepoHttpBuilder {

    private String username;
    private String password;
    private String authHost;

    /**
     * Basic constructor.
     */
    public FcrepoHttpBuilder() {
    }

    /**
     * Static construction method
     * @return a FcrepoHttpBuilder
     */
    public static FcrepoHttpBuilder create() {
        return new FcrepoHttpBuilder();
    }

    /**
     * Build a http client with the appropriate credentials (if provided).
     * @return the client.
     */
    public CloseableHttpClient buildClient() {
        final HttpClientBuilder clientBuilder = HttpClients.custom();
        if (getAuthUsername() != null && getAuthHost() != null) {
            clientBuilder.setDefaultCredentialsProvider(getCredentialsProvider());
        }
        return clientBuilder.build();
    }

    /**
     * Build a http client context with the configured host.
     * @return the context
     */
    public HttpClientContext buildContext() {
        final HttpClientContext httpContext = HttpClientContext.create();
        if (getAuthUsername() != null && getAuthHost() != null) {
            httpContext.setCredentialsProvider(getCredentialsProvider());
            httpContext.setAuthCache(getAuthCache());
        }
        return httpContext;
    }

    /**
     * Set the username
     * @param username the username
     * @return this builder.
     */
    public FcrepoHttpBuilder setCredentials(final String username, final String password) {
        if (username != null) {
            this.username = username;
        }
        if (password != null) {
            this.password = password;
        }
        return this;
    }

    /**
     * @return the username
     */
    private String getAuthUsername() {
        return username;
    }

    /**
     * @return the password
     */
    private String getAuthPassword() {
        return password;
    }

    /**
     * Set the hostname for authentication.
     * @param authHost the hostname
     * @return this builder
     */
    public FcrepoHttpBuilder setAuthHost(final String authHost) {
        if (authHost != null) {
            this.authHost = authHost;
        }
        return this;
    }

    /**
     * @return the hostname
     */
    private String getAuthHost() {
        return authHost;
    }

    /**
     * Build a credentials provider if you have the necessary information.
     * @return credentials provider or null if none.
     */
    private CredentialsProvider getCredentialsProvider() {
        if (getAuthUsername() != null) {
            final AuthScope authScope;
            if (getAuthHost() != null) {
                final HttpHost fcrepoUrl = HttpHost.create(getAuthHost());
                authScope = new AuthScope(fcrepoUrl);
            } else {
                authScope = AuthScope.ANY;
            }
            final Credentials credentials = new UsernamePasswordCredentials(getAuthUsername(), getAuthPassword());
            final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(authScope, credentials);
            return credentialsProvider;
        }
        return null;
    }

    /**
     * Build an auth cache for a http client.
     * @return the auth cache.
     */
    private AuthCache getAuthCache() {
        final HttpHost fcrepoUrl = HttpHost.create(getAuthHost());
        final BasicAuthCache authCache = new BasicAuthCache();
        final AuthScheme basicAuth = new BasicScheme();
        authCache.put(fcrepoUrl, basicAuth);
        return authCache;
    }

}
