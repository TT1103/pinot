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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StreamCSVMessageDecoderTest {
  @Test
  public void testCSVMessageDecoder() throws Exception {
    StreamCSVMessageDecoder decoder = new StreamCSVMessageDecoder();

    HashMap<String, String> props = new HashMap<>();
    props.put("csvHeaderString", "a,b,c");
    props.put("csvDelimiter", ",");

    Set<String> fieldsToRead = new HashSet<>(Arrays.asList("a", "b", "c"));
    decoder.init(props, fieldsToRead, "test");
    GenericRow row = decoder.decode("1,abc,321".getBytes(UTF_8), new GenericRow());

    Assert.assertEquals((String) row.getValue("a"), "1");
    Assert.assertEquals(row.getValue("b"), "abc");
    Assert.assertEquals(row.getValue("c"), "321");

    row = decoder.decode("123,invalid,a".getBytes(UTF_8), new GenericRow());
    System.out.println(row);
  }
}
