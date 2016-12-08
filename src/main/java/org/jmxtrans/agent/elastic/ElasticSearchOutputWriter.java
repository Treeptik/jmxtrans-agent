/**
 * The MIT License
 * Copyright © 2013 Evgeniy Khyst
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.jmxtrans.agent.elastic;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import org.jmxtrans.agent.AbstractOutputWriter;
import org.jmxtrans.agent.elastic.util.IndexNameBuilder;
import org.jmxtrans.agent.elastic.util.TypeNameCreator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.jmxtrans.agent.util.ConfigurationUtils.getInt;
import static org.jmxtrans.agent.util.ConfigurationUtils.getString;

/**
 *
 * @author Nicolas Muller inspired by Evgeniy Khyst
 */
public class ElasticSearchOutputWriter extends AbstractOutputWriter {

    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    
    private static final String ELASTICSEARCH_HOST = "elasticsearchHost";
    private static final String ELASTICSEARCH_HOST_DEFAULT_VALUE = "localhost";
    
    private static final String ELASTICSEARCH_PORT = "elasticsearchPort";
    private static final int ELASTICSEARCH_PORT_DEFAULT_VALUE = 9300;
    
    private static final String ELASTICSEARCH_CLUSTER_NAME = "elasticsearchClusterName";
    private static final String ELASTICSEARCH_CLUSTER_NAME_DEFAULT_VALUE = "elasticsearch";
    
    private static final String ELASTICSEARCH_INDEX = "elasticsearchIndex";
    private static final String ELASTICSEARCH_INDEX_DEFAULT_VALUE = "jmxtrans-%{yyyy.MM.dd}";
    
    private static final String USE_PREFIX_AS_TYPE = "usePrefixAsType";
    private static final boolean USE_PREFIX_AS_TYPE_DEFAULT_VALUE = true;
    
    private static final String TYPE_DEFAULT_VALUE = "jmxtrans";
    
    private static final String NODE_NAME = "nodeName";

    private IndexNameBuilder indexNameBuilder;
    private boolean usePrefixAsType;
    
    private String host;
    private String nodeName;

    private JestClient jestClient;

    private JestClient createJestClient(String connectionUrl) {
        if (logger.isLoggable(getTraceLevel())) {
            logger.log(getTraceLevel(), "Create a jest elastic search client for connection url " + connectionUrl);
        }
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(
                new HttpClientConfig.Builder(connectionUrl)
                        .multiThreaded(true)
                        .build());
        return factory.getObject();
    }

    private ThreadLocal<Map<String, Map<String, Object>>> documents = new ThreadLocal<Map<String, Map<String, Object>>>() {

        @Override
        protected Map<String, Map<String, Object>> initialValue() {
            return new HashMap<>();
        }
    };
    
    @Override
    public void postConstruct(Map<String, String> settings) {
        super.postConstruct(settings);

        try {
            String elasticSearchHost = getString(settings, ELASTICSEARCH_HOST, ELASTICSEARCH_HOST_DEFAULT_VALUE);
            int elasticSearchPort = getInt(settings, ELASTICSEARCH_PORT, ELASTICSEARCH_PORT_DEFAULT_VALUE);
            String elasticsearchClusterName = getString(settings, ELASTICSEARCH_CLUSTER_NAME, ELASTICSEARCH_CLUSTER_NAME_DEFAULT_VALUE);

            try {
                System.out.println("Connection to http://"+elasticSearchHost+":"+elasticSearchPort);
                jestClient = createJestClient("http://"+elasticSearchHost+":"+elasticSearchPort);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (logger.isLoggable(getTraceLevel())) {
                String msg = String.format("ElasticSearchOutputWriter is configured with elasticHost=%s, elasticPort=%s", elasticSearchHost, elasticSearchPort);
                logger.log(getTraceLevel(), msg);
            }

            String indexNamePattern = getString(settings, ELASTICSEARCH_INDEX, ELASTICSEARCH_INDEX_DEFAULT_VALUE);
            indexNameBuilder = new IndexNameBuilder(indexNamePattern);

            usePrefixAsType = Boolean.parseBoolean(
                    getString(settings, USE_PREFIX_AS_TYPE, String.valueOf(USE_PREFIX_AS_TYPE_DEFAULT_VALUE)));

            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

            nodeName = getString(settings, NODE_NAME, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void preDestroy() {
        jestClient.shutdownClient();
    }

    @Override
    public void writeQueryResult(String name, String type, Object value) throws IOException {
        Map<String, Map<String, Object>> currentThreadDocuments = documents.get();
        String documentType = getType(name);
        if (!currentThreadDocuments.containsKey(documentType)) {
            currentThreadDocuments.put(documentType, newDocument());
        }
        Map<String, Object> document = currentThreadDocuments.get(documentType);
        document.put(name, value);
    }
    
    @Override
    public void writeInvocationResult(String invocationName, Object value) throws IOException {
        writeQueryResult(invocationName, null, value);
    }

    @Override
    public void postCollect() throws IOException {
        Date timestamp = new Date();
        Map<String, Map<String, Object>> currentThreadDocuments = documents.get();
        for (Map.Entry<String, Map<String, Object>> entry : currentThreadDocuments.entrySet()) {
            String type = entry.getKey();
            if (type != null) {
                Map<String, Object> document = entry.getValue();
                document.put("@timestamp", new SimpleDateFormat(TIMESTAMP_FORMAT).format(timestamp));
                Index index = new Index.Builder(document).index(indexNameBuilder.build(timestamp)).type(type).build();
                JestResult addToIndex = jestClient.execute(index);
                if (!addToIndex.isSucceeded()) {
                    String msgError = String.format("Unable to write entry to elastic: %s", addToIndex.getErrorMessage());
                    System.out.println(msgError);
                    logger.log(getInfoLevel(), msgError);
                } else {
                    if (logger.isLoggable(getTraceLevel())) {
                        logger.log(getTraceLevel(), addToIndex.getJsonString());
                    }
                }
            }
        }
        documents.remove();
    }
    
    private String getType(String name) {
        return usePrefixAsType ? TypeNameCreator.fromPrefix(name) : TYPE_DEFAULT_VALUE;
    }

    private Map<String, Object> newDocument() {
        Map<String, Object> document = new HashMap<>();
        if (host != null) {
            document.put("host", host);
        }
        if (nodeName != null) {
            document.put("nodeName", nodeName);
        }
        return document;
    }
}
