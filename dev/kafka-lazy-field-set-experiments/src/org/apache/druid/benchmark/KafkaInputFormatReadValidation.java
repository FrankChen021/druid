/*
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

package org.apache.druid.benchmark;

import org.openjdk.jmh.infra.Blackhole;

import java.lang.reflect.Field;

/** Runs the benchmark setup and one alternating pair for each timed case before JMH measurement. */
public class KafkaInputFormatReadValidation
{
  private static final String BLACKHOLE_PASSWORD =
      "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.";

  private static final Case[] CASES = {
      new Case(256, "explicit", "regular"),
      new Case(4096, "explicit", "regular"),
      new Case(256, "explicit", "metadataRepeated"),
      new Case(256, "discovery", "regular")
  };

  public static void main(final String[] args) throws Exception
  {
    final String implementation = args.length == 0 ? "implementation" : args[0];
    final Blackhole blackhole = new Blackhole(BLACKHOLE_PASSWORD);
    for (final Case benchmarkCase : CASES) {
      final KafkaInputFormatReadBenchmark benchmark = new KafkaInputFormatReadBenchmark();
      setField(benchmark, "messageSize", benchmarkCase.messageSize);
      setField(benchmark, "dimensionMode", benchmarkCase.dimensionMode);
      setField(benchmark, "accessMode", benchmarkCase.accessMode);
      setField(benchmark, "timestampPattern", "alternating");
      benchmark.setUp();
      benchmark.parseAndRead(blackhole);
      benchmark.parseAndRead(blackhole);
    }
    System.out.println("Validated KafkaInputFormatReadBenchmark values and metadata for " + implementation);
  }

  private static void setField(final Object target, final String fieldName, final Object value) throws Exception
  {
    final Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static class Case
  {
    private final int messageSize;
    private final String dimensionMode;
    private final String accessMode;

    private Case(final int messageSize, final String dimensionMode, final String accessMode)
    {
      this.messageSize = messageSize;
      this.dimensionMode = dimensionMode;
      this.accessMode = accessMode;
    }
  }
}
