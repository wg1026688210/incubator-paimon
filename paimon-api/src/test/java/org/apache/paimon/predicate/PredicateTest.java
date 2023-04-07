/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.predicate;

import org.apache.paimon.format.FieldStats;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Predicate}s. */
public class PredicateTest {

    @Test
    public void testEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.equal(0, 5);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 6, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.notEqual(0, 5));
    }

    @Test
    public void testEqualNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.equal(0, null);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.notEqual(0, 5);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 6, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(5, 5, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.equal(0, 5));
    }

    @Test
    public void testNotEqualNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.notEqual(0, null);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testGreater() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.greaterThan(0, 5);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {6})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 4, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 6, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.lessOrEqual(0, 5));
    }

    @Test
    public void testGreaterNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.greaterThan(0, null);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 4, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterOrEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.greaterOrEqual(0, 5);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {6})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 4, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 6, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.lessThan(0, 5));
    }

    @Test
    public void testGreaterOrEqualNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.greaterOrEqual(0, null);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 4, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testLess() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.lessThan(0, 5);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {6})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(4, 7, 0L)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.greaterOrEqual(0, 5));
    }

    @Test
    public void testLessNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.lessThan(0, null);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testLessOrEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.lessOrEqual(0, 5);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {6})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(4, 7, 0L)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.greaterThan(0, 5));
    }

    @Test
    public void testLessOrEqualNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.lessOrEqual(0, null);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testIsNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.isNull(0);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(true);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 1L)})).isEqualTo(true);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.isNotNull(0));
    }

    @Test
    public void testIsNotNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.isNotNull(0);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 1L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(null, null, 3L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.isNull(0));
    }

    @Test
    public void testIn() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.in(0, Arrays.asList(1, 3));
        assertThat(predicate).isInstanceOf(CompoundPredicate.class);

        assertThat(predicate.test(new Object[] {1})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {2})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testInNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.in(0, Arrays.asList(1, null, 3));
        assertThat(predicate).isInstanceOf(CompoundPredicate.class);

        assertThat(predicate.test(new Object[] {1})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {2})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testNotIn() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.notIn(0, Arrays.asList(1, 3));
        assertThat(predicate).isInstanceOf(CompoundPredicate.class);

        assertThat(predicate.test(new Object[] {1})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {2})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {3})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(1, 1, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(3, 3, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(1, 3, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testNotInNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.notIn(0, Arrays.asList(1, null, 3));
        assertThat(predicate).isInstanceOf(CompoundPredicate.class);

        assertThat(predicate.test(new Object[] {1})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {2})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(1, 1, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(3, 3, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(1, 3, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testLargeIn() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        List<Object> literals = new ArrayList<>();
        literals.add(1);
        literals.add(3);
        for (int i = 10; i < 30; i++) {
            literals.add(i);
        }
        Predicate predicate = builder.in(0, literals);
        assertThat(predicate).isInstanceOf(LeafPredicate.class);

        assertThat(predicate.test(new Object[] {1})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {2})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(29, 32, 0L)}))
                .isEqualTo(true);
    }

    @Test
    public void testLargeInNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        List<Object> literals = new ArrayList<>();
        literals.add(1);
        literals.add(null);
        literals.add(3);
        for (int i = 10; i < 30; i++) {
            literals.add(i);
        }
        Predicate predicate = builder.in(0, literals);
        assertThat(predicate).isInstanceOf(LeafPredicate.class);

        assertThat(predicate.test(new Object[] {1})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {2})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(29, 32, 0L)}))
                .isEqualTo(true);
    }

    @Test
    public void testLargeNotIn() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        List<Object> literals = new ArrayList<>();
        literals.add(1);
        literals.add(3);
        for (int i = 10; i < 30; i++) {
            literals.add(i);
        }
        Predicate predicate = builder.notIn(0, literals);
        assertThat(predicate).isInstanceOf(LeafPredicate.class);

        assertThat(predicate.test(new Object[] {1})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {2})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {3})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(1, 1, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(3, 3, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(1, 3, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(29, 32, 0L)}))
                .isEqualTo(true);
    }

    @Test
    public void testLargeNotInNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        List<Object> literals = new ArrayList<>();
        literals.add(1);
        literals.add(null);
        literals.add(3);
        for (int i = 10; i < 30; i++) {
            literals.add(i);
        }
        Predicate predicate = builder.notIn(0, literals);
        assertThat(predicate).isInstanceOf(LeafPredicate.class);

        assertThat(predicate.test(new Object[] {1})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {2})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(1, 1, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(3, 3, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(1, 3, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(29, 32, 0L)}))
                .isEqualTo(false);
    }

    @Test
    public void testAnd() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType(), new IntType()));
        Predicate predicate = PredicateBuilder.and(builder.equal(0, 3), builder.equal(1, 5));

        assertThat(predicate.test(new Object[] {4, 5})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3, 6})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3, 5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null, 5})).isEqualTo(false);

        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(3, 6, 0L), new FieldStats(4, 6, 0L)
                                }))
                .isEqualTo(true);
        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(3, 6, 0L), new FieldStats(6, 8, 0L)
                                }))
                .isEqualTo(false);
        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(6, 7, 0L), new FieldStats(4, 6, 0L)
                                }))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null))
                .isEqualTo(PredicateBuilder.or(builder.notEqual(0, 3), builder.notEqual(1, 5)));
    }

    @Test
    public void testOr() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType(), new IntType()));
        Predicate predicate = PredicateBuilder.or(builder.equal(0, 3), builder.equal(1, 5));

        assertThat(predicate.test(new Object[] {4, 6})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3, 6})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {3, 5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null, 5})).isEqualTo(true);

        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(3, 6, 0L), new FieldStats(4, 6, 0L)
                                }))
                .isEqualTo(true);
        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(3, 6, 0L), new FieldStats(6, 8, 0L)
                                }))
                .isEqualTo(true);
        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(6, 7, 0L), new FieldStats(8, 10, 0L)
                                }))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null))
                .isEqualTo(PredicateBuilder.and(builder.notEqual(0, 3), builder.notEqual(1, 5)));
    }

    @Test
    public void testUnknownStats() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.equal(0, 5);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(null, null, 3L)}))
                .isEqualTo(false);

        // unknown stats, we don't know, likely to hit
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(null, null, 4L)}))
                .isEqualTo(true);
    }
}
