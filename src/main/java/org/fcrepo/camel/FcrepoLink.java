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
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * A class representing the value of an HTTP Link header
 *
 * Originally in fcrepo-camel, then fcrepo-java-client, now back in fcrepo-camel.
 * See org.fcrepo.client.FcrepoLink
 *
 * @author Aaron Coburn
 * @author bbpennel
 */
public class FcrepoLink {
    private static final String PARAM_DELIM = ";";
    private static final String META_REL = "rel";
    private static final String META_TYPE = "type";
    private URI uri;
    private Map<String, String> params;

    /**
     * Constructor from a link string.
     * @param link the link
     */
    public FcrepoLink(final String link) {
        if (link == null) {
            throw new IllegalArgumentException("Link header did not contain a URI");
        } else {
            this.params = new HashMap<>();
            this.parse(link);
        }
    }

    /**
     * Constructor from a uri and parameters.
     * @param uri the uri
     * @param params the parameters
     */
    private FcrepoLink(final URI uri, final Map<String, String> params) {
        this.uri = uri;
        this.params = params;
    }

    /**
     * @return the uri
     */
    public URI getUri() {
        return this.uri;
    }

    /**
     * @return the "rel" parameter or null.
     */
    public String getRel() {
        return this.getParam(META_REL);
    }

    /**
     * @return the "type" parameter or null.
     */
    public String getType() {
        return this.getParam(META_TYPE);
    }

    /**
     * Return the specified parameter.
     * @param name the parameter name.
     * @return the parameter value or null if it doesn't exist
     */
    public String getParam(final String name) {
        return this.params.get(name);
    }

    /**
     * Get all the parameters.
     * @return all the parameters.
     */
    public Map<String, String> getParams() {
        return this.params;
    }

    /**
     * Parse a full link header.
     * @param link the link header as a string
     */
    private void parse(final String link) {
        final int paramIndex = link.indexOf(PARAM_DELIM);
        if (paramIndex == -1) {
            this.uri = getLinkPart(link);
        } else {
            this.uri = getLinkPart(link.substring(0, paramIndex));
            this.parseParams(link.substring(paramIndex + 1));
        }

    }

    /**
     * Parse the parameters from the link header
     * @param paramString the parameter portion of the link header.
     */
    private void parseParams(final String paramString) {
        final StringTokenizer st = new StringTokenizer(paramString, PARAM_DELIM + "\",", true);

        while (st.hasMoreTokens()) {
            boolean inQuotes = false;
            final StringBuilder paramBuilder = new StringBuilder();

            String token;
            while (st.hasMoreTokens()) {
                token = st.nextToken();
                if (token.equals("\"")) {
                    inQuotes = !inQuotes;
                } else {
                    if (!inQuotes && token.equals(PARAM_DELIM)) {
                        break;
                    }

                    if (!inQuotes && token.equals(",")) {
                        throw new IllegalArgumentException("Cannot parse link, contains unterminated quotes");
                    }

                    paramBuilder.append(token);
                }
            }

            if (inQuotes) {
                throw new IllegalArgumentException("Cannot parse link, contains unterminated quotes");
            }

            token = paramBuilder.toString();
            final String[] components = token.split("=", 2);
            if (components.length != 2) {
                throw new IllegalArgumentException("Cannot parse link, improperly structured parameter encountered: " +
                        token);
            }

            this.params.put(components[0].trim(), components[1].trim());
        }

    }

    /**
     * Strip quotation marks from the beginning and end of a string.
     * @param value the string
     * @return the string with quotations marks removed.
     */
    private static String stripQuotes(final String value) {
        return value.startsWith("\"") && value.endsWith("\"") ? value.substring(1, value.length() - 1) : value;
    }

    /**
     * Rebuild a Link header string.
     * @return the link header
     */
    public String toString() {
        final StringBuilder result = new StringBuilder();
        result.append('<').append(this.uri.toString()).append('>');
        this.params.forEach((name, value) -> {
            result.append("; ").append(name).append("=\"").append(value).append('"');
        });
        return result.toString();
    }

    /**
     * Get the URI from a link header by removing < and > from around it.
     * @param uriPart the URI part
     * @return the URI
     */
    private static URI getLinkPart(final String uriPart) {
        final String linkPart = uriPart.trim();
        if (linkPart.startsWith("<") && linkPart.endsWith(">")) {
            return URI.create(linkPart.substring(1, linkPart.length() - 1));
        } else {
            throw new IllegalArgumentException("Link header did not contain a URI");
        }
    }

    /**
     * Static creation method.
     * @param link the link header string
     * @return a FcrepoLink
     */
    public static FcrepoLink valueOf(final String link) {
        return new FcrepoLink(link);
    }

    /**
     * Static builder class.
     */
    public static class Builder {
        private URI uri;
        private Map<String, String> params = new HashMap<>();

        /**
         * Constructor
         */
        public Builder() {
        }

        /**
         * Add a uri
         * @param uri the uri
         * @return this builder
         */
        public FcrepoLink.Builder uri(final URI uri) {
            this.uri = uri;
            return this;
        }

        /**
         * Add a uri
         * @param uri the uri
         * @return this builder
         */
        public FcrepoLink.Builder uri(final String uri) {
            this.uri = URI.create(uri);
            return this;
        }

        /**
         * Add a "rel" parameter
         * @param rel the parameter value
         * @return this builder
         */
        public FcrepoLink.Builder rel(final String rel) {
            return this.param(META_REL, rel);
        }

        /**
         * Add a "type" parameter
         * @param type the parameter value
         * @return this builder
         */
        public FcrepoLink.Builder type(final String type) {
            return this.param(META_TYPE, type);
        }

        /**
         * Add a parameter
         * @param name the parameter name
         * @param value the parameter value
         * @return this builder
         */
        public FcrepoLink.Builder param(final String name, final String value) {
            this.params.put(name, FcrepoLink.stripQuotes(value));
            return this;
        }

        /**
         * @return A FcrepoLink based on the values set for this Builder.
         */
        public FcrepoLink build() {
            return new FcrepoLink(this.uri, this.params);
        }
    }
}
