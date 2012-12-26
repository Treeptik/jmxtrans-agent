/*
 * Copyright 2008-2012 Xebia and the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jmxexporter;

import org.junit.Test;

import javax.management.ObjectName;


import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * @author <a href="mailto:cleclerc@xebia.fr">Cyrille Le Clerc</a>
 */
public class ResultNameStrategyTest {

    ResultNameStrategy strategy = new ResultNameStrategy();

    @Test
    public void testResolveExpression() throws Exception {
        // prepare
        String expression = "tomcat.datasource.%host%.%path%.%name%";
        String objectName = "Catalina:type=Resource,resourcetype=Context,path=/,host=localhost,class=javax.sql.DataSource,name=\"jdbc/my-datasource\"";

        // test
        String actual = strategy.resolveExpression(expression, new ObjectName(objectName));

        // verify
        assertThat(actual, is("tomcat.datasource.localhost._.jdbc_my_datasource"));
    }

    @Test
    public void testEscapeObjectName1() throws Exception {
        String objectName = "java.lang:type=GarbageCollector,name=PS Scavenge";
        String actual = strategy.escapeObjectName(new ObjectName(objectName));
        assertThat(actual, is("java_lang.name__PS_Scavenge.type__GarbageCollector"));
    }

    @Test
    public void testEscapeObjectName2() throws Exception {
        String objectName = "Catalina:type=Resource,resourcetype=Context,path=/,host=localhost,class=javax.sql.DataSource,name=\"jdbc/my-datasource\"";
        String actual = strategy.escapeObjectName(new ObjectName(objectName));
        assertThat(actual, is("Catalina.class__javax_sql_DataSource.host__localhost.name__jdbc_my_datasource.path___.resourcetype__Context.type__Resource"));
    }
}