/**
 * The MIT License (MIT)
 * Copyright (c) 2017 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.cosmosdb.spark;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;
import org.joda.time.Instant;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class IteratorLogger implements Serializable {
    private final static String headerLine = "Timestamp|Level|Event|PKRangeId|UserAgentSuffix|Message|Exception|" +
            "IteratedDocuments|Count";

    private final static String LOG_LEVEL_INFO = "I";
    private final static String LOG_LEVEL_ERROR = "E";

    private final static String EVENT_NAME_LOG = "Log";
    private final static String EVENT_NAME_ITERATOR_NEXT = "ConsumedFromIterator";
    private final CosmosLogWriter writer;
    private final String userAgentSuffix;
    private final String pkRangeId;
    private final String iteratorName;
    private final StringBuilder iteratedDocuments = new StringBuilder().append("[");
    private final AtomicInteger iteratedDocumentCount = new AtomicInteger(0);

    public IteratorLogger(
            CosmosLogWriter writer,
            String userAgentSuffix,
            String pkRangeId,
            String iteratorName) {
        this.userAgentSuffix = userAgentSuffix;
        this.pkRangeId = pkRangeId;
        this.iteratorName = iteratorName;
        this.writer = writer;

        if (writer != null) {
            this.writer.writeLine(headerLine);
        }
    }

    public void logError(String message, Throwable throwable) {
        if (writer == null) {
            return;
        }

        logLogEvent(LOG_LEVEL_ERROR, message, throwable);
    }

    public <T extends Resource> void onIteratorNext(T document, PartitionKeyDefinition pkDefinition) {
        if (writer == null) {
            return;
        }

        String contentToFlush = null;
        int countSnapshot;

        synchronized (this.iteratedDocuments) {
            if (this.iteratedDocuments.length() > 1) {
                this.iteratedDocuments.append(", ");
            }
            this.iteratedDocuments.append(
                    formatDocumentIdentity(
                            DocumentAnalyzer.extractDocumentIdentity(
                                    document,
                                    pkDefinition)));
            countSnapshot = this.iteratedDocumentCount.incrementAndGet();

            if (this.iteratedDocuments.length() > 1024) {
                this.iteratedDocuments.append("]");
                contentToFlush = this.iteratedDocuments.toString();
                this.iteratedDocuments.setLength(0);
                this.iteratedDocuments.append("[");
                this.iteratedDocumentCount.set(0);
            }
        }

        if (contentToFlush != null) {
            this.logLine(
                    LOG_LEVEL_INFO,
                    EVENT_NAME_ITERATOR_NEXT,
                    this.iteratorName,
                    null,
                    contentToFlush,
                    countSnapshot);
        }
    }

    public void flush() {
        if (this.writer == null) {
            return;
        }

        synchronized (this.iteratedDocuments) {
            if (this.iteratedDocuments.length() > 1) {
                this.logLine(
                        LOG_LEVEL_INFO,
                        EVENT_NAME_ITERATOR_NEXT,
                        this.iteratorName,
                        null,
                        this.iteratedDocuments.toString() + "]",
                        this.iteratedDocumentCount.get());
            }
        }
    }

    private <T extends Resource> String extractDocumentIdentities(List<T> resources, PartitionKeyDefinition pkDefinition) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < resources.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }

            String[] identity = DocumentAnalyzer.extractDocumentIdentity(
                    resources.get(i),
                    pkDefinition);
            sb.append(formatDocumentIdentity(identity));
        }
        sb.append("]");

        return sb.toString();
    }

    private String formatDocumentIdentity(String[] identity) {
        return  "(" + identity[0] + "/" + identity[1] + ")";
    }

    private void logLogEvent(
            String logLevel,
            String message,
            Throwable exception) {

        logLine(
                logLevel,
                EVENT_NAME_LOG,
                message,
                exception,
                null,
                null);
    }

    // "Timestamp|Level|Event|PKRangeId|UserAgentSuffix|Message|Exception|" +
    // "IteratedDocuments|Count";
    private void logLine(
            String logLevel,
            String eventName,
            String message,
            Throwable exception,
            String iteratedDocuments,
            Integer count) {

        writer.writeLine(
                join(
                        Instant.now().toString(),
                        logLevel,
                        eventName,
                        pkRangeId,
                        this.userAgentSuffix,
                        message,
                        throwableToString(exception),
                        iteratedDocuments,
                        count != null ? count.toString() : "")
        );
    }

    private String join(String... args) {
        StringBuilder sb = new StringBuilder();
        String separator = "|";
        for (String c : args) {
            if (sb.length() > 0) {
                sb.append(separator);
            }

            sb.append(c);
        }

        return sb.toString();
    }

    private String throwableToString(Throwable throwable) {
        String exceptionText = null;
        if (throwable == null) {
            return null;
        }

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }

    private static class DocumentAnalyzer {
        private final static Logger LOGGER = LoggerFactory.getLogger(DocumentAnalyzer.class);

        /**
         * Extracts effective {@link PartitionKeyInternal} from serialized document.
         * @param partitionKeyDefinition Information about partition key.
         * @return PartitionKeyInternal
         */
        public static String[] extractDocumentIdentity(
                Resource root,
                PartitionKeyDefinition partitionKeyDefinition)  {

            String pk = "n/a";
            if (partitionKeyDefinition != null && partitionKeyDefinition.getPaths().size() > 0) {
                pk = DocumentAnalyzer
                        .extractPartitionKeyValueInternal(
                                root,
                                partitionKeyDefinition).toJson();
            }

            String id = "n/a";
            if (root.getId() != null){
                id = root.getId();
            }

            return new String[] { pk, id };
        }

        private static PartitionKeyInternal extractPartitionKeyValueInternal(
                Resource resource,
                PartitionKeyDefinition partitionKeyDefinition) {
            if (partitionKeyDefinition != null) {
                String path = partitionKeyDefinition.getPaths().iterator().next();
                Collection<String> parts = com.microsoft.azure.documentdb.internal.PathParser.getPathParts(path);
                if (parts.size() >= 1) {
                    Object value = resource.getObjectByPath(parts);
                    if (value == null || value.getClass() == JSONObject.class) {
                        value = Undefined.Value();
                    }

                    return PartitionKeyInternal.fromObjectArray(Arrays.asList(value), false);
                }
            }

            return null;
        }

        public static PartitionKeyInternal fromPartitionKeyvalue(Object partitionKeyValue) {
            try {
                return PartitionKeyInternal.fromObjectArray(Collections.singletonList(partitionKeyValue), true);
            } catch (Exception e) {
                LOGGER.error("Failed to instantiate ParitionKeyInternal from {}", partitionKeyValue, e);
                throw toRuntimeException(e);
            }
        }

        public static RuntimeException toRuntimeException(Exception e) {
            if (e instanceof RuntimeException) {
                return (RuntimeException) e;
            } else {
                return new RuntimeException(e);
            }
        }
    }

    final static class PathParser
    {
        private final static char SEGMENT_SEPARATOR = '/';
        private final static String ERROR_MESSAGE_FORMAT = "Invalid path \"%s\", failed at %d";

        /**
         * Extract parts from a given path for '/' as the separator.
         * <p>
         * This code doesn't do as much validation as the backend, as it assumes that IndexingPolicy path coming from the backend is valid.
         *
         * @param path specifies a partition key given as a path.
         * @return a list of all the parts for '/' as the separator.
         */
        public static List<String> getPathParts(String path)
        {
            List<String> tokens = new ArrayList<String>();
            AtomicInteger currentIndex = new AtomicInteger();

            while (currentIndex.get() < path.length())
            {
                char currentChar = path.charAt(currentIndex.get());
                if (currentChar != SEGMENT_SEPARATOR)
                {
                    throw new IllegalArgumentException(
                            String.format(ERROR_MESSAGE_FORMAT, path, currentIndex.get()));
                }

                if (currentIndex.incrementAndGet() == path.length())
                {
                    break;
                }

                currentChar = path.charAt(currentIndex.get());
                if (currentChar == '\"' || currentChar == '\'')
                {
                    // Handles the partial path given in quotes such as "'abc/def'"
                    tokens.add(getEscapedToken(path, currentIndex));
                }
                else
                {
                    tokens.add(getToken(path, currentIndex));
                }
            }

            return tokens;
        }

        private static String getEscapedToken(String path, AtomicInteger currentIndex)
        {
            char quote = path.charAt(currentIndex.get());
            int newIndex = currentIndex.incrementAndGet();

            while (true)
            {
                newIndex = path.indexOf(quote, newIndex);
                if (newIndex == -1)
                {
                    throw new IllegalArgumentException(
                            String.format(ERROR_MESSAGE_FORMAT, path, currentIndex.get()));
                }

                // Ignore escaped quote in the partial path we look at such as "'abc/def \'12\'/ghi'"
                if (path.charAt(newIndex - 1) != '\\')
                {
                    break;
                }

                ++newIndex;
            }

            String token = path.substring(currentIndex.get(), newIndex);
            currentIndex.set(newIndex + 1);

            return token;
        }

        private static String getToken(String path, AtomicInteger currentIndex)
        {
            int newIndex = path.indexOf(SEGMENT_SEPARATOR, currentIndex.get());
            String token = null;
            if (newIndex == -1)
            {
                token = path.substring(currentIndex.get());
                currentIndex.set(path.length());
            }
            else
            {
                token = path.substring(currentIndex.get(), newIndex);
                currentIndex.set(newIndex);
            }

            token = token.trim();
            return token;
        }
    }
}