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

package org.apache.druid.query.expression;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionValidationException;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.JupiterAssertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class IPv4AddressMatchExprMacroTest extends MacroTestBase
{
  private static final Expr IPV4 = ExprEval.ofString("192.168.0.1").toExpr();
  private static final Expr IPV4_LONG = ExprEval.of(3232235521L).toExpr();
  private static final Expr IPV4_UINT = ExprEval.ofString("3232235521").toExpr();
  private static final Expr IPV4_NETWORK = ExprEval.ofString("192.168.0.0").toExpr();
  private static final Expr IPV4_BROADCAST = ExprEval.ofString("192.168.255.255").toExpr();
  private static final Expr IPV6_COMPATIBLE = ExprEval.ofString("::192.168.0.1").toExpr();
  private static final Expr IPV6_MAPPED = ExprEval.ofString("::ffff:192.168.0.1").toExpr();
  private static final Expr SUBNET_192_168 = ExprEval.ofString("192.168.0.0/16").toExpr();
  private static final Expr SUBNET_10 = ExprEval.ofString("10.0.0.0/8").toExpr();
  private static final Expr NOT_LITERAL = Parser.parse("\"notliteral\"", ExprMacroTable.nil());

  public IPv4AddressMatchExprMacroTest()
  {
    super(new IPv4AddressMatchExprMacro());
  }

  @Test
  public void testTooFewArgs()
  {
    expectException(ExpressionValidationException.class, "requires 2 arguments");

    apply(Collections.emptyList());
  }

  @Test
  public void testTooManyArgs()
  {
    expectException(ExpressionValidationException.class, "requires 2 arguments");

    apply(Arrays.asList(IPV4, SUBNET_192_168, NOT_LITERAL));
  }

  @Test
  public void testSubnetArgNotLiteral()
  {
    expectException(ExpressionValidationException.class, "subnet argument must be a literal");

    apply(Arrays.asList(IPV4, NOT_LITERAL));
  }

  @Test
  public void testSubnetArgInvalid()
  {
    expectException(IllegalArgumentException.class, "subnet arg has an invalid format");

    Expr invalidSubnet = ExprEval.ofString("192.168.0.1/invalid").toExpr();
    apply(Arrays.asList(IPV4, invalidSubnet));
  }

  @Test
  public void testNullStringArg()
  {
    Expr nullString = ExprEval.ofString(null).toExpr();
    JupiterAssertions.assertFalse(eval(nullString, SUBNET_192_168));
  }

  @Test
  public void testNullLongArg()
  {
    Expr nullLong = ExprEval.ofLong(null).toExpr();
    JupiterAssertions.assertFalse(eval(nullLong, SUBNET_192_168));
  }

  @Test
  public void testInvalidArgType()
  {
    Expr longArray = ExprEval.ofLongArray(new Long[]{1L, 2L}).toExpr();
    JupiterAssertions.assertFalse(eval(longArray, SUBNET_192_168));
  }

  @Test
  public void testMatchingStringArgIPv4()
  {
    JupiterAssertions.assertTrue(eval(IPV4, SUBNET_192_168));
  }

  @Test
  public void testNotMatchingStringArgIPv4()
  {
    JupiterAssertions.assertFalse(eval(IPV4, SUBNET_10));
  }

  @Test
  public void testMatchingStringArgIPv6Mapped()
  {
    JupiterAssertions.assertFalse(eval(IPV6_MAPPED, SUBNET_192_168));
  }

  @Test
  public void testNotMatchingStringArgIPv6Mapped()
  {
    JupiterAssertions.assertFalse(eval(IPV6_MAPPED, SUBNET_10));
  }

  @Test
  public void testMatchingStringArgIPv6Compatible()
  {
    JupiterAssertions.assertFalse(eval(IPV6_COMPATIBLE, SUBNET_192_168));
  }

  @Test
  public void testNotMatchingStringArgIPv6Compatible()
  {
    JupiterAssertions.assertFalse(eval(IPV6_COMPATIBLE, SUBNET_10));
  }

  @Test
  public void testNotIpAddress()
  {
    Expr notIpAddress = ExprEval.ofString("druid.apache.org").toExpr();
    JupiterAssertions.assertFalse(eval(notIpAddress, SUBNET_192_168));
  }

  @Test
  public void testMatchingLongArg()
  {
    JupiterAssertions.assertTrue(eval(IPV4_LONG, SUBNET_192_168));
  }

  @Test
  public void testNotMatchingLongArg()
  {
    JupiterAssertions.assertFalse(eval(IPV4_LONG, SUBNET_10));
  }

  @Test
  public void testMatchingStringArgUnsignedInt()
  {
    JupiterAssertions.assertFalse(eval(IPV4_UINT, SUBNET_192_168));
  }

  @Test
  public void testNotMatchingStringArgUnsignedInt()
  {
    JupiterAssertions.assertFalse(eval(IPV4_UINT, SUBNET_10));
  }

  @Test
  public void testInclusive()
  {
    Expr subnet = SUBNET_192_168;
    JupiterAssertions.assertTrue(eval(IPV4_NETWORK, subnet));
    JupiterAssertions.assertTrue(eval(IPV4, subnet));
    JupiterAssertions.assertTrue(eval(IPV4_BROADCAST, subnet));
  }

  @Test
  public void testMatchesPrefix()
  {
    JupiterAssertions.assertTrue(eval(ExprEval.ofString("192.168.1.250").toExpr(), ExprEval.ofString("192.168.1.251/31").toExpr()));
    JupiterAssertions.assertFalse(eval(ExprEval.ofString("192.168.1.240").toExpr(), ExprEval.ofString("192.168.1.251/31").toExpr()));
    JupiterAssertions.assertFalse(eval(ExprEval.ofString("192.168.1.250").toExpr(), ExprEval.ofString("192.168.1.251/32").toExpr()));
    JupiterAssertions.assertTrue(eval(ExprEval.ofString("192.168.1.251").toExpr(), ExprEval.ofString("192.168.1.251/32").toExpr()));

    JupiterAssertions.assertTrue(eval(
        ExprEval.of(IPv4AddressExprUtils.parse("192.168.1.250").longValue()).toExpr(),
        ExprEval.ofString("192.168.1.251/31").toExpr()
    ));
    JupiterAssertions.assertFalse(eval(
        ExprEval.of(IPv4AddressExprUtils.parse("192.168.1.240").longValue()).toExpr(),
        ExprEval.ofString("192.168.1.251/31").toExpr()
    ));
    JupiterAssertions.assertFalse(eval(
        ExprEval.of(IPv4AddressExprUtils.parse("192.168.1.250").longValue()).toExpr(),
        ExprEval.ofString("192.168.1.251/32").toExpr()
    ));
    JupiterAssertions.assertTrue(eval(
        ExprEval.of(IPv4AddressExprUtils.parse("192.168.1.251").longValue()).toExpr(),
        ExprEval.ofString("192.168.1.251/32").toExpr()
    ));
  }

  private boolean eval(Expr... args)
  {
    Expr expr = apply(Arrays.asList(args));
    ExprEval eval = expr.eval(InputBindings.nilBindings());
    return eval.asBoolean();
  }
}
