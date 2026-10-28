package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Throughput of the batch conversion plus what every downstream operator does with a row first (serializing it
 * to a {@link BinaryRowData}), on a lineitem-like batch: {@code ./mvnw -Pperf test -Dtest=ArrowRowConverterPerfTest}.
 */
@Tag("perf")
class ArrowRowConverterPerfTest {

    private static final int ROWS = 8192;
    private static final int ROUNDS = 400;

    @Test
    void columnarConversion() {
        RowType type = (RowType) DataTypes.ROW(DataTypes.FIELD("k", DataTypes.BIGINT()),
                DataTypes.FIELD("q", DataTypes.INT()), DataTypes.FIELD("p", DataTypes.DECIMAL(15, 2)),
                DataTypes.FIELD("d", DataTypes.DOUBLE()), DataTypes.FIELD("s", DataTypes.STRING()),
                DataTypes.FIELD("t", DataTypes.DATE())).getLogicalType();
        RowDataSerializer serializer = new RowDataSerializer(type);
        try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = batch(allocator)) {
            ArrowRowConverter converter = new ArrowRowConverter(type);
            long checksum = 0;
            for (int warmup = 0; warmup < 2; warmup++) {
                checksum = run(converter, root, serializer);
            }
            long start = System.nanoTime();
            long result = run(converter, root, serializer);
            long nanos = System.nanoTime() - start;
            assertEquals(checksum, result);
            System.out.printf("columnar conversion + BinaryRowData: %.1f M rows/s%n",
                    (double) ROWS * ROUNDS / nanos * 1000);
        }
    }

    /**
     * Converts the batch {@link #ROUNDS} times and serializes every row.
     *
     * @param converter converter
     * @param root batch
     * @param serializer row serializer
     * @return checksum of the serialized rows
     */
    private static long run(ArrowRowConverter converter, VectorSchemaRoot root, RowDataSerializer serializer) {
        long sum = 0;
        for (int round = 0; round < ROUNDS; round++) {
            VectorizedColumnBatch batch = converter.convert(root);
            for (int r = 0; r < batch.getNumRows(); r++) {
                RowData row = new ColumnarRowData(batch, r);
                sum += serializer.toBinaryRow(row).getSizeInBytes();
            }
        }
        return sum;
    }

    /**
     * Builds a lineitem-like batch.
     *
     * @param allocator allocator
     * @return batch
     */
    static VectorSchemaRoot batch(BufferAllocator allocator) {
        BigIntVector k = new BigIntVector("k", allocator);
        IntVector q = new IntVector("q", allocator);
        DecimalVector p = new DecimalVector("p", allocator, 15, 2);
        Float8Vector d = new Float8Vector("d", allocator);
        VarCharVector s = new VarCharVector("s", allocator);
        DateDayVector t = new DateDayVector("t", allocator);
        List<FieldVector> vectors = List.of(k, q, p, d, s, t);
        vectors.forEach(v -> v.setInitialCapacity(ROWS));
        vectors.forEach(FieldVector::allocateNew);
        for (int r = 0; r < ROWS; r++) {
            k.set(r, r * 31L);
            q.set(r, r % 50);
            p.set(r, BigDecimal.valueOf(r * 7L, 2));
            d.set(r, r / 100.0);
            s.setSafe(r, ("comment " + r % 1000).getBytes(StandardCharsets.UTF_8));
            t.set(r, 8000 + r % 2500);
        }
        vectors.forEach(v -> v.setValueCount(ROWS));
        VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        root.setRowCount(ROWS);
        return root;
    }
}
