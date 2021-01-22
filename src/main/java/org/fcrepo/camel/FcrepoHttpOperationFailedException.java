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

import java.net.URI;

/**
 * A failure exception for Fcrepo Camel Http requests to Fedora.
 *
 * Based off the FcrepoOperationFailedException from fcrepo-java-client.
 * see org.fcrepo.client.FcrepoOperationFailedException
 *
 * @author whikloj
 */
public class FcrepoHttpOperationFailedException extends Exception {

    private final URI url;
    private final int statusCode;
    private final String statusText;

    /**
     * Basic constructor
     * @param url the URL being operated on
     * @param statusCode the received status code
     * @param statusText the received status message or null.
     */
    public FcrepoHttpOperationFailedException(final URI url, final int statusCode, final String statusText) {
        super("HTTP operation failed invoking " + (url != null ? url.toString() : "[null]") + " with statusCode: " +
                statusCode + " and message: " + statusText);
        this.url = url;
        this.statusCode = statusCode;
        this.statusText = statusText;
    }

    /**
     * @return the URI being operated on
     */
    public URI getUrl() {
        return this.url;
    }

    /**
     * @return the received status code
     */
    public int getStatusCode() {
        return this.statusCode;
    }

    /**
     * @return the received status message or null if none.
     */
    public String getStatusText() {
        return this.statusText;
    }
}
