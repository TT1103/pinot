/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.plugin.inputformat.csv;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StreamCSVMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamCSVMessageDecoder.class);
  private static final char DEFAULT_DELIMITER = ',';
  private static final char DEFAULT_MULTI_VALUE_DELIMTER = '`';
  private static final String HEADER_CONFIG_KEY = "csvHeaderString";
  private static final String DELIMITER_CONFIG_KEY = "csvDelimiter";
  private static final String MULTI_VALUE_DELIMITER_KEY = "csvMultiValueDelimiter";

  private RecordExtractor<CSVRecord> _csvRecordExtractor;
  private CSVFormat _csvFormat;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {

    Set<String> columnNames = new LinkedHashSet<>();
    char multi_val_delimiter;
    char delimiter;
    try {
      // parse the csv header that defines the order of the incoming rows
      String headerStr = props.get(HEADER_CONFIG_KEY);
      CSVParser csvParser = CSVParser.parse(headerStr, CSVFormat.newFormat(DEFAULT_DELIMITER));
      csvParser.iterator().next().iterator().forEachRemaining(columnNames::add);

      // load the multivalue delimiter and delimiter from properties if it exists
      multi_val_delimiter = props.getOrDefault(MULTI_VALUE_DELIMITER_KEY, String.valueOf(DEFAULT_MULTI_VALUE_DELIMTER)).charAt(0);
      delimiter = props.getOrDefault(DELIMITER_CONFIG_KEY, String.valueOf(DEFAULT_DELIMITER)).charAt(0);

      _csvFormat = CSVFormat.newFormat(delimiter).withHeader(Arrays.stream(columnNames.toArray()).toArray(String[]::new));
    } catch (Exception e) {
      LOGGER.error("Failed to load props for CSV stream ingestion.", e);
      return;
    }

    CSVRecordExtractorConfig config = new CSVRecordExtractorConfig();
    config.setMultiValueDelimiter(multi_val_delimiter);
    config.setColumnNames(columnNames);
    _csvRecordExtractor = new CSVRecordExtractor();
    _csvRecordExtractor.init(fieldsToRead, config);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      CSVParser parser = CSVParser.parse(new String(payload, UTF_8), _csvFormat);
      _csvRecordExtractor.extract(parser.iterator().next(), destination);
      return destination;
    } catch (Exception e) {
      LOGGER.error("Caught exception while decoding row, discarding row. Payload is {}", new String(payload), e);
      return null;
    }
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }
}
