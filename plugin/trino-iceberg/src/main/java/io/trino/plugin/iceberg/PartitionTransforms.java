/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Murmur3Hash32;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.PartitionField;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.trino.plugin.iceberg.util.Timestamps.getTimestampTz;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.Decimals.isLongDecimal;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_HOUR;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Integer.parseInt;
import static java.lang.Math.floorDiv;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

public final class PartitionTransforms
{
    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket\\[(\\d+)]");
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");

    private static final DateTimeField YEAR_FIELD = ISOChronology.getInstanceUTC().year();
    private static final DateTimeField MONTH_FIELD = ISOChronology.getInstanceUTC().monthOfYear();

    private PartitionTransforms() {}

    public static ColumnTransform getColumnTransform(PartitionField field, Type type)
    {
        String transform = field.transform().toString();

        switch (transform) {
            case "identity":
                return identity(type);
            case "year":
                if (type.equals(DATE)) {
                    return yearsFromDate();
                }
                if (type.equals(TIMESTAMP_MICROS)) {
                    return yearsFromTimestamp();
                }
                if (type.equals(TIMESTAMP_TZ_MICROS)) {
                    return yearsFromTimestampWithTimeZone();
                }
                throw new UnsupportedOperationException("Unsupported type for 'year': " + field);
            case "month":
                if (type.equals(DATE)) {
                    return monthsFromDate();
                }
                if (type.equals(TIMESTAMP_MICROS)) {
                    return monthsFromTimestamp();
                }
                if (type.equals(TIMESTAMP_TZ_MICROS)) {
                    return monthsFromTimestampWithTimeZone();
                }
                throw new UnsupportedOperationException("Unsupported type for 'month': " + field);
            case "day":
                if (type.equals(DATE)) {
                    return daysFromDate();
                }
                if (type.equals(TIMESTAMP_MICROS)) {
                    return daysFromTimestamp();
                }
                if (type.equals(TIMESTAMP_TZ_MICROS)) {
                    return daysFromTimestampWithTimeZone();
                }
                throw new UnsupportedOperationException("Unsupported type for 'day': " + field);
            case "hour":
                if (type.equals(TIMESTAMP_MICROS)) {
                    return hoursFromTimestamp();
                }
                if (type.equals(TIMESTAMP_TZ_MICROS)) {
                    return hoursFromTimestampWithTimeZone();
                }
                throw new UnsupportedOperationException("Unsupported type for 'hour': " + field);
            case "void":
                return voidTransform(type);
        }

        Matcher matcher = BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            int count = parseInt(matcher.group(1));
            return bucket(type, count);
        }

        matcher = TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            int width = parseInt(matcher.group(1));
            if (type.equals(INTEGER)) {
                return truncateInteger(width);
            }
            if (type.equals(BIGINT)) {
                return truncateBigint(width);
            }
            if (isShortDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return truncateShortDecimal(type, width, decimal);
            }
            if (isLongDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return truncateLongDecimal(type, width, decimal);
            }
            if (type instanceof VarcharType) {
                return truncateVarchar(width);
            }
            if (type.equals(VARBINARY)) {
                return truncateVarbinary(width);
            }
            throw new UnsupportedOperationException("Unsupported type for 'truncate': " + field);
        }

        throw new UnsupportedOperationException("Unsupported partition transform: " + field);
    }

    private static ColumnTransform identity(Type type)
    {
        return new ColumnTransform(type, Function.identity());
    }

    private static ColumnTransform bucket(Type type, int count)
    {
        return new ColumnTransform(
                INTEGER,
                getBucketTransform(type, count));
    }

    public static Function<Block, Block> getBucketTransform(Type type, int count)
    {
        if (type.equals(INTEGER)) {
            return block -> bucketInteger(block, count);
        }
        if (type.equals(BIGINT)) {
            return block -> bucketBigint(block, count);
        }
        if (isShortDecimal(type)) {
            DecimalType decimal = (DecimalType) type;
            return block -> bucketShortDecimal(decimal, block, count);
        }
        if (isLongDecimal(type)) {
            DecimalType decimal = (DecimalType) type;
            return block -> bucketLongDecimal(decimal, block, count);
        }
        if (type.equals(DATE)) {
            return block -> bucketDate(block, count);
        }
        if (type.equals(TIME_MICROS)) {
            return block -> bucketTime(block, count);
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return block -> bucketTimestamp(block, count);
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return block -> bucketTimestampWithTimeZone(block, count);
        }
        if (type instanceof VarcharType) {
            return block -> bucketVarchar(block, count);
        }
        if (type.equals(VARBINARY)) {
            return block -> bucketVarbinary(block, count);
        }
        if (type.equals(UUID)) {
            return block -> bucketUuid(block, count);
        }
        throw new UnsupportedOperationException("Unsupported type for 'bucket': " + type);
    }

    private static ColumnTransform yearsFromDate()
    {
        return new ColumnTransform(
                INTEGER,
                block -> extractDate(block, value -> epochYear(DAYS.toMillis(value))));
    }

    private static ColumnTransform monthsFromDate()
    {
        return new ColumnTransform(
                INTEGER,
                block -> extractDate(block, value -> epochMonth(DAYS.toMillis(value))));
    }

    private static ColumnTransform daysFromDate()
    {
        return new ColumnTransform(
                INTEGER,
                block -> extractDate(block, LongUnaryOperator.identity()));
    }

    private static Block extractDate(Block block, LongUnaryOperator function)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = DATE.getLong(block, position);
            INTEGER.writeLong(builder, function.applyAsLong(value));
        }
        return builder.build();
    }

    private static ColumnTransform yearsFromTimestamp()
    {
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestamp(block, PartitionTransforms::epochYear));
    }

    private static ColumnTransform monthsFromTimestamp()
    {
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestamp(block, PartitionTransforms::epochMonth));
    }

    private static ColumnTransform daysFromTimestamp()
    {
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestamp(block, PartitionTransforms::epochDay));
    }

    private static ColumnTransform hoursFromTimestamp()
    {
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestamp(block, PartitionTransforms::epochHour));
    }

    private static Block extractTimestamp(Block block, LongUnaryOperator function)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long epochMicros = TIMESTAMP_MICROS.getLong(block, position);
            long epochMillis = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
            INTEGER.writeLong(builder, function.applyAsLong(epochMillis));
        }
        return builder.build();
    }

    private static ColumnTransform yearsFromTimestampWithTimeZone()
    {
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestampWithTimeZone(block, PartitionTransforms::epochYear));
    }

    private static ColumnTransform monthsFromTimestampWithTimeZone()
    {
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestampWithTimeZone(block, PartitionTransforms::epochMonth));
    }

    private static ColumnTransform daysFromTimestampWithTimeZone()
    {
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestampWithTimeZone(block, PartitionTransforms::epochDay));
    }

    private static ColumnTransform hoursFromTimestampWithTimeZone()
    {
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestampWithTimeZone(block, PartitionTransforms::epochHour));
    }

    private static Block extractTimestampWithTimeZone(Block block, LongUnaryOperator function)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long epochMillis = getTimestampTz(block, position).getEpochMillis();
            INTEGER.writeLong(builder, function.applyAsLong(epochMillis));
        }
        return builder.build();
    }

    private static Block bucketInteger(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(INTEGER.getLong(block, position)));
    }

    private static Block bucketBigint(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(BIGINT.getLong(block, position)));
    }

    private static Block bucketShortDecimal(DecimalType decimal, Block block, int count)
    {
        return bucketBlock(block, count, position -> {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, position);
            return bucketHash(Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        });
    }

    private static Block bucketLongDecimal(DecimalType decimal, Block block, int count)
    {
        return bucketBlock(block, count, position -> {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, position);
            return bucketHash(Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        });
    }

    private static Block bucketDate(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(DATE.getLong(block, position)));
    }

    private static Block bucketTime(Block block, int count)
    {
        return bucketBlock(block, count, position -> {
            long picos = TIME_MICROS.getLong(block, position);
            return bucketHash(picos / PICOSECONDS_PER_MICROSECOND);
        });
    }

    private static Block bucketTimestamp(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(((Type) TIMESTAMP_MICROS).getLong(block, position)));
    }

    private static Block bucketTimestampWithTimeZone(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(timestampTzToMicros(getTimestampTz(block, position))));
    }

    private static Block bucketVarchar(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(VARCHAR.getSlice(block, position)));
    }

    private static Block bucketVarbinary(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(VARBINARY.getSlice(block, position)));
    }

    private static Block bucketUuid(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(UUID.getSlice(block, position)));
    }

    private static Block bucketBlock(Block block, int count, IntUnaryOperator hasher)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            int hash = hasher.applyAsInt(position);
            int bucket = (hash & Integer.MAX_VALUE) % count;
            INTEGER.writeLong(builder, bucket);
        }
        return builder.build();
    }

    private static int bucketHash(long value)
    {
        return Murmur3Hash32.hash(value);
    }

    private static int bucketHash(Slice value)
    {
        return Murmur3Hash32.hash(value);
    }

    private static ColumnTransform truncateInteger(int width)
    {
        return new ColumnTransform(
                INTEGER,
                block -> truncateInteger(block, width));
    }

    private static Block truncateInteger(Block block, int width)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = INTEGER.getLong(block, position);
            long truncated = value - ((value % width) + width) % width;
            INTEGER.writeLong(builder, truncated);
        }
        return builder.build();
    }

    private static ColumnTransform truncateBigint(int width)
    {
        return new ColumnTransform(
                BIGINT,
                block -> truncateBigint(block, width));
    }

    private static Block truncateBigint(Block block, int width)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = BIGINT.getLong(block, position);
            long truncated = value - ((value % width) + width) % width;
            BIGINT.writeLong(builder, truncated);
        }
        return builder.build();
    }

    private static ColumnTransform truncateShortDecimal(Type type, int width, DecimalType decimal)
    {
        return new ColumnTransform(
                type,
                block -> truncateShortDecimal(decimal, block, width));
    }

    private static Block truncateShortDecimal(DecimalType type, Block block, int width)
    {
        BigInteger unscaledWidth = BigInteger.valueOf(width);
        BlockBuilder builder = type.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(type, block, position);
            BigDecimal truncated = truncateDecimal(value, unscaledWidth);
            type.writeLong(builder, encodeShortScaledValue(truncated, type.getScale()));
        }
        return builder.build();
    }

    private static ColumnTransform truncateLongDecimal(Type type, int width, DecimalType decimal)
    {
        return new ColumnTransform(
                type,
                block -> truncateLongDecimal(decimal, block, width));
    }

    private static Block truncateLongDecimal(DecimalType type, Block block, int width)
    {
        BigInteger unscaledWidth = BigInteger.valueOf(width);
        BlockBuilder builder = type.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(type, block, position);
            BigDecimal truncated = truncateDecimal(value, unscaledWidth);
            type.writeSlice(builder, encodeScaledValue(truncated, type.getScale()));
        }
        return builder.build();
    }

    private static BigDecimal truncateDecimal(BigDecimal value, BigInteger unscaledWidth)
    {
        BigDecimal remainder = new BigDecimal(
                value.unscaledValue()
                        .remainder(unscaledWidth)
                        .add(unscaledWidth)
                        .remainder(unscaledWidth),
                value.scale());
        return value.subtract(remainder);
    }

    private static ColumnTransform truncateVarchar(int width)
    {
        return new ColumnTransform(
                VARCHAR,
                block -> truncateVarchar(block, width));
    }

    private static Block truncateVarchar(Block block, int max)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Slice value = VARCHAR.getSlice(block, position);
            Slice truncated = truncateVarchar(value, max);
            VARCHAR.writeSlice(builder, truncated);
        }
        return builder.build();
    }

    private static Slice truncateVarchar(Slice value, int max)
    {
        if (value.length() <= max) {
            return value;
        }
        int end = offsetOfCodePoint(value, 0, max);
        if (end < 0) {
            return value;
        }
        return value.slice(0, end);
    }

    private static ColumnTransform truncateVarbinary(int width)
    {
        return new ColumnTransform(
                VARBINARY,
                block -> truncateVarbinary(block, width));
    }

    private static Block truncateVarbinary(Block block, int max)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Slice value = VARBINARY.getSlice(block, position);
            Slice truncated = (value.length() <= max) ? value : value.slice(0, max);
            VARBINARY.writeSlice(builder, truncated);
        }
        return builder.build();
    }

    private static ColumnTransform voidTransform(Type type)
    {
        Block nullBlock = nativeValueToBlock(type, null);
        return new ColumnTransform(
                type,
                block -> new RunLengthEncodedBlock(nullBlock, block.getPositionCount()));
    }

    @VisibleForTesting
    static long epochYear(long epochMilli)
    {
        return YEAR_FIELD.get(epochMilli) - 1970;
    }

    @VisibleForTesting
    static long epochMonth(long epochMilli)
    {
        long year = epochYear(epochMilli);
        int month = MONTH_FIELD.get(epochMilli) - 1;
        return (year * 12) + month;
    }

    @VisibleForTesting
    static long epochDay(long epochMilli)
    {
        return floorDiv(epochMilli, MILLISECONDS_PER_DAY);
    }

    @VisibleForTesting
    static long epochHour(long epochMilli)
    {
        return floorDiv(epochMilli, MILLISECONDS_PER_HOUR);
    }

    public static class ColumnTransform
    {
        private final Type type;
        private final Function<Block, Block> transform;

        public ColumnTransform(Type type, Function<Block, Block> transform)
        {
            this.type = requireNonNull(type, "type is null");
            this.transform = requireNonNull(transform, "transform is null");
        }

        public Type getType()
        {
            return type;
        }

        public Function<Block, Block> getTransform()
        {
            return transform;
        }
    }
}
