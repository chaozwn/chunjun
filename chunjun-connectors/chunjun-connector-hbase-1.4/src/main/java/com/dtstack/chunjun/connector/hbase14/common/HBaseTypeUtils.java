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

package com.dtstack.chunjun.connector.hbase14.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/** A utility class to process data exchange with HBase type system. */
@Internal
public class HBaseTypeUtils {

    private static final byte[] EMPTY_BYTES = new byte[] {};

    private static final int MIN_TIMESTAMP_PRECISION = 0;
    private static final int MAX_TIMESTAMP_PRECISION = 3;
    private static final int MIN_TIME_PRECISION = 0;
    private static final int MAX_TIME_PRECISION = 3;

    /** Deserialize byte array to Java Object with the given type. */
    public static Object deserializeToObject(byte[] value, int typeIdx, Charset stringCharset) {
        switch (typeIdx) {
            case 0: // byte[]
                return value;
            case 1: // String
                return Arrays.equals(EMPTY_BYTES, value) ? null : new String(value, stringCharset);
            case 2: // byte
                return value[0];
            case 3:
                return Bytes.toShort(value);
            case 4:
                return Bytes.toInt(value);
            case 5:
                return Bytes.toLong(value);
            case 6:
                return Bytes.toFloat(value);
            case 7:
                return Bytes.toDouble(value);
            case 8:
                return Bytes.toBoolean(value);
            case 9: // sql.Timestamp encoded as long
                return new Timestamp(Bytes.toLong(value));
            case 10: // sql.Date encoded as long
                return new Date(Bytes.toLong(value));
            case 11: // sql.Time encoded as long
                return new Time(Bytes.toLong(value));
            case 12:
                return Bytes.toBigDecimal(value);
            case 13:
                return new BigInteger(value);

            default:
                throw new IllegalArgumentException("unsupported type index:" + typeIdx);
        }
    }

    /** Serialize the Java Object to byte array with the given type. */
    public static byte[] serializeFromObject(Object value, int typeIdx, Charset stringCharset) {
        switch (typeIdx) {
            case 0: // byte[]
                return (byte[]) value;
            case 1: // external String
                return value == null ? EMPTY_BYTES : ((String) value).getBytes(stringCharset);
            case 2: // byte
                return value == null ? EMPTY_BYTES : new byte[] {(byte) value};
            case 3:
                return Bytes.toBytes((short) value);
            case 4:
                return Bytes.toBytes((int) value);
            case 5:
                return Bytes.toBytes((long) value);
            case 6:
                return Bytes.toBytes((float) value);
            case 7:
                return Bytes.toBytes((double) value);
            case 8:
                return Bytes.toBytes((boolean) value);
            case 9: // sql.Timestamp encoded to Long
                return Bytes.toBytes(((Timestamp) value).getTime());
            case 10: // sql.Date encoded as long
                return Bytes.toBytes(((Date) value).getTime());
            case 11: // sql.Time encoded as long
                return Bytes.toBytes(((Time) value).getTime());
            case 12:
                return Bytes.toBytes((BigDecimal) value);
            case 13:
                return ((BigInteger) value).toByteArray();

            default:
                throw new IllegalArgumentException("unsupported type index:" + typeIdx);
        }
    }

    /**
     * Gets the type index (type representation in HBase connector) from the {@link
     * TypeInformation}.
     */
    public static int getTypeIndex(TypeInformation typeInfo) {
        return getTypeIndex(typeInfo.getTypeClass());
    }

    /** Checks whether the given Class is a supported type in HBase connector. */
    public static boolean isSupportedType(Class<?> clazz) {
        return getTypeIndex(clazz) != -1;
    }

    private static int getTypeIndex(Class<?> clazz) {
        if (byte[].class.equals(clazz)) {
            return 0;
        } else if (String.class.equals(clazz)) {
            return 1;
        } else if (Byte.class.equals(clazz)) {
            return 2;
        } else if (Short.class.equals(clazz)) {
            return 3;
        } else if (Integer.class.equals(clazz)) {
            return 4;
        } else if (Long.class.equals(clazz)) {
            return 5;
        } else if (Float.class.equals(clazz)) {
            return 6;
        } else if (Double.class.equals(clazz)) {
            return 7;
        } else if (Boolean.class.equals(clazz)) {
            return 8;
        } else if (Timestamp.class.equals(clazz)) {
            return 9;
        } else if (Date.class.equals(clazz)) {
            return 10;
        } else if (Time.class.equals(clazz)) {
            return 11;
        } else if (BigDecimal.class.equals(clazz)) {
            return 12;
        } else if (BigInteger.class.equals(clazz)) {
            return 13;
        } else {
            return -1;
        }
    }

    /** Checks whether the given {@link LogicalType} is supported in HBase connector. */
    public static boolean isSupportedType(LogicalType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
            case BIGINT:
            case INTERVAL_DAY_TIME:
            case FLOAT:
            case DOUBLE:
                return true;
            case TIME_WITHOUT_TIME_ZONE:
                final int timePrecision = getPrecision(type);
                if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of TIME type is out of the range [%s, %s] supported by "
                                            + "HBase connector",
                                    timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
                }
                return true;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(type);
                if (timestampPrecision < MIN_TIMESTAMP_PRECISION
                        || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by "
                                            + "HBase connector",
                                    timestampPrecision,
                                    MIN_TIMESTAMP_PRECISION,
                                    MAX_TIMESTAMP_PRECISION));
                }
                return true;
            case TIMESTAMP_WITH_TIME_ZONE:
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
            case STRUCTURED_TYPE:
            case DISTINCT_TYPE:
            case RAW:
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
                return false;
            default:
                throw new IllegalArgumentException();
        }
    }
}
