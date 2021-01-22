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

import static org.fcrepo.camel.processor.ProcessorUtils.tokenizePropertyPlaceholder;

import java.util.List;
import java.util.Properties;

import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

/**
 * @author acoburn
 */
public class ProcessorUtilsTest extends CamelTestSupport {

    @Override
    protected Properties useOverridePropertiesWithPropertiesComponent() {
         final Properties props = new Properties();
         props.put("test.prop1", "one,two,three");
         props.put("test.prop2", "    four   ,   five\n,six ");
         props.put("test.prop3", "seven");
         props.put("test.prop4", "eight,\n\t\tnine,\n   ten");
         props.put("test.prop5", "");
         props.put("test.prop6", "    ");
         return props;
    }

    @Test
    public void testPropertyTokenizerSimple() {
        final List<String> list1 = tokenizePropertyPlaceholder(context, "{{test.prop1}}", "\\s*,\\s*");
        assertEquals(3, list1.size());
        assertTrue(list1.contains("one"));
        assertTrue(list1.contains("two"));
        assertTrue(list1.contains("three"));
    }

    @Test
    public void testPropertyTokenizerWhitespace1() {
        final List<String> list2 = tokenizePropertyPlaceholder(context, "{{test.prop2}}", "\\s*,\\s*");
        assertEquals(3, list2.size());
        assertTrue(list2.contains("four"));
        assertTrue(list2.contains("five"));
        assertTrue(list2.contains("six"));
    }

    @Test
    public void testPropertyTokenizerSingleton() {
        final List<String> list3 = tokenizePropertyPlaceholder(context, "{{test.prop3}}", "\\s*,\\s*");
        assertEquals(1, list3.size());
        assertTrue(list3.contains("seven"));
    }

    @Test
    public void testPropertyTokenizerWhitespace2() {
        final List<String> list4 = tokenizePropertyPlaceholder(context, "{{test.prop4}}", "\\s*,\\s*");
        assertEquals(3, list4.size());
        assertTrue(list4.contains("eight"));
        assertTrue(list4.contains("nine"));
        assertTrue(list4.contains("ten"));
    }

    @Test
    public void testPropertyTokenizerEmpty1() {
        final List<String> list5 = tokenizePropertyPlaceholder(context, "{{test.prop5}}", "\\s*,\\s*");
        assertEquals(0, list5.size());
    }

    @Test
    public void testPropertyTokenizerEmpty2() {
        final List<String> list6 = tokenizePropertyPlaceholder(context, "{{test.prop6}}", "\\s*,\\s*");
        assertEquals(0, list6.size());
    }

    @Test
    public void testPropertyTokenizerNotDefined() {
        final List<String> list7 = tokenizePropertyPlaceholder(context, "{{test.prop7}}", "\\s*,\\s*");
        assertEquals(0, list7.size());
    }
}
