/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene.util.fv;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

final class FeaturePositionTokenFilter extends TokenFilter {

    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private int tokenCount = 0;

    FeaturePositionTokenFilter(TokenStream stream) {
      super(stream);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        tokenCount++;
        String token = new String(termAttribute.buffer(), 0, termAttribute.length());
        termAttribute.setEmpty();
        termAttribute.append(String.valueOf(tokenCount));
        termAttribute.append("_");
        termAttribute.append(token);
        return true;
      } else {
        return false;
      }
    }

  @Override
  public void reset() throws IOException {
    super.reset();
    tokenCount = 0;
  }

  }