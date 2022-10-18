package it.unimi.dsi.law;

import com.google.common.primitives.Bytes;
import it.unimi.dsi.law.persistence.*;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.keyvalue.DefaultMapEntry;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.assertj.core.data.MapEntry;
import org.assertj.core.util.Files;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.params.MainNetParams;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PersistenceLayerUnitTest {
    @Mock TransactionOutPoint top_a, top_b, top_c;
    @Mock Sha256Hash sha_a, sha_b, sha_c;
    @Mock Address addr;

    static PersistenceLayer pl;
    static IncompleteMappings im;
    static AddressConversion ac;
    static TransactionOutpointFilter tof;

    static RocksDB db;
    static List<ColumnFamilyHandle> columns;

    @BeforeAll
    static void setup() throws RocksDBException, NoSuchFieldException, IllegalAccessException, IOException {
        File temp = Files.newTemporaryFolder();
        temp.deleteOnExit();

        pl = PersistenceLayer.getInstance(temp.getAbsolutePath());

        Field fdb = PersistenceLayer.class.getDeclaredField("db");
        fdb.setAccessible(true);
        db = (RocksDB) fdb.get(pl);

        im = pl.getIncompleteMappings();
        ac = pl.getAddressConversion();
        tof = pl.getTransactionOutpointFilter();

        columns = List.of(
            (ColumnFamilyHandle) extract(ac, "column"),
            (ColumnFamilyHandle) extract(im, "column"),
            (ColumnFamilyHandle) extract(tof, "column")
        );
    }

    @ParameterizedTest
    @MethodSource("provideLongs")
    void convertFromLong(long l) {
        byte[] bb = ByteConversion.long2bytes(l);
        assertThat(ByteConversion.bytes2long(bb)).isEqualTo(l);
    }

    @ParameterizedTest
    @MethodSource("provideInts")
    void convertFromInt(int i) {
        byte[] bb = ByteConversion.int2bytes(i);
        assertThat(ByteConversion.bytes2int(bb)).isEqualTo(i);
    }

    @ParameterizedTest
    @MethodSource("provideByteArrays")
    void convertFromBytes(byte[] bb) {
        long l = ByteConversion.bytes2long(bb);
        assertThat(ByteConversion.long2bytes(l)).containsExactly(Bytes.ensureCapacity(bb, 8, 0));
    }

    @ParameterizedTest
    @MethodSource("provideLongList")
    void convertFromLongList(List<Long> ll) {
        byte[] bb = ByteConversion.longList2bytes(ll);
        assertThat(ByteConversion.bytes2longList(bb)).isEqualTo(ll);
    }

    @ParameterizedTest
    @MethodSource("provideByteList")
    void convertFromByteList(byte[] bb) {
        List<Long> ll = ByteConversion.bytes2longList(bb);
        assertThat(ByteConversion.longList2bytes(ll)).isEqualTo(bb);
    }

    @ParameterizedTest
    @MethodSource("provideByteArrays")
    void concatenationByteArray(byte[] bb) {
        byte[] bba = Arrays.copyOfRange(bb, 0, bb.length / 2);
        byte[] bbb = Arrays.copyOfRange(bb, bb.length / 2, bb.length);

        assertThat(Bytes.concat(bba, bbb)).isEqualTo(bb);
    }

    @ParameterizedTest
    @MethodSource("provideLongList")
    void putIncompleteMapping(List<Long> ll) throws RocksDBException {
        im.put(top_a, ll);
        assertThat(im.get(top_a)).containsExactlyElementsOf(ll);
    }

    @ParameterizedTest
    @MethodSource("provideLongList")
    void multiplePutIncompleteMapping(List<Long> ll) throws RocksDBException {
        List<Long> ll1 = ll.subList(0, ll.size() / 2);
        List<Long> ll2 = ll.subList(ll.size() / 2, ll.size());

        im.put(top_a, ll1);
        im.put(top_a, ll2);

        assertThat(im.get(top_a)).containsExactlyInAnyOrder(ll.toArray(Long[]::new));
    }

    @ParameterizedTest
    @MethodSource("provideLongList")
    void multipleEmptyPutIncompleteMapping(List<Long> ll) throws RocksDBException {
        im.put(top_a, ll);
        im.put(top_a, List.of());
        im.put(top_a, List.of());
        im.put(top_a, List.of());

        assertThat(im.get(top_a)).containsExactly(ll.toArray(Long[]::new));
    }

    @ParameterizedTest
    @MethodSource("provideIntList")
    void consistencyAddressConversion(List<Integer> ii) throws RocksDBException {
        Set<Long> ids = new HashSet<>();
        for (int i : ii) {
            when(addr.getHash()).thenReturn(intToHash(i).getBytes());
            ids.add(ac.mapAddress(addr));
        }

        assertThat(ac).extracting("count")
                .isEqualTo((long) ids.size())
                .isEqualTo(ii.stream().distinct().count());
    }

    @ParameterizedTest
    @MethodSource("provideIntList")
    void consistencySingleAddressConversion(List<Integer> ii) throws RocksDBException {
        Set<Long> firstMapping = new HashSet<>();
        for (int i : ii) {
            when(addr.getHash()).thenReturn(intToHash(i).getBytes());
            firstMapping.add(ac.mapAddress(addr));
        }

        Set<Long> recallMapping = new HashSet<>();
        for (int i : ii) {
            when(addr.getHash()).thenReturn(intToHash(i).getBytes());
            recallMapping.add(ac.mapAddress(addr));
        }

        assertThat(firstMapping).containsExactly(recallMapping.toArray(Long[]::new));
    }

    @ParameterizedTest
    @MethodSource("provideLongList")
    void rocksDbWorksLikeMultiValuedMap(List<Long> ll) throws RocksDBException {
        MultiValuedMap<TransactionOutPoint, Long> mvm = new HashSetValuedHashMap<>();
        mvm.putAll(top_a, ll);
        mvm.putAll(top_b, ll);
        mvm.putAll(top_c, ll);

        im.put(top_a, ll);
        im.put(top_b, ll);
        im.put(top_c, ll);

        assertThat(im.get(top_a)).containsExactly(ll.toArray(Long[]::new));
        assertThat(im.get(top_b)).containsExactly(ll.toArray(Long[]::new));
        assertThat(im.get(top_c)).containsExactly(ll.toArray(Long[]::new));
    }

    @ParameterizedTest
    @MethodSource("provideLongList")
    void rocksDbWorksLikeMultiValuedMap2(List<Long> ll) throws RocksDBException {
        MultiValuedMap<TransactionOutPoint, Long> mvm = new HashSetValuedHashMap<>();
        mvm.putAll(top_a, ll);
        mvm.putAll(top_a, ll);
        mvm.putAll(top_a, ll);

        im.put(top_a, ll);
        im.put(top_a, ll);
        im.put(top_a, ll);

        List<Long> lll = new ArrayList<>(ll);
        lll.addAll(ll);
        lll.addAll(ll);

        assertThat(im.get(top_a)).containsExactly(lll.toArray(Long[]::new));
    }

    @ParameterizedTest
    @MethodSource("provideLongListLongListLongList")
    void rocksDbWorksLikeMultiValuedMap3(List<List<Long>> ll) throws RocksDBException {
        List<Long> l1 = ll.get(0), l2 = ll.get(1), l3 = ll.get(2);

        MultiValuedMap<TransactionOutPoint, Long> mvm = new ArrayListValuedHashMap<>();
        mvm.putAll(top_a, l1);
        mvm.putAll(top_b, l2);
        mvm.putAll(top_c, l3);

        im.put(top_a, l1);
        im.put(top_b, l2);
        im.put(top_c, l3);

        assertThat(im.get(top_a)).containsExactlyInAnyOrder(mvm.get(top_a).toArray(Long[]::new));
        assertThat(im.get(top_b)).containsExactlyInAnyOrder(mvm.get(top_b).toArray(Long[]::new));
        assertThat(im.get(top_c)).containsExactlyInAnyOrder(mvm.get(top_c).toArray(Long[]::new));
    }

    @ParameterizedTest
    @MethodSource("provideInts")
    void transactionOutpointFilter(Integer n) throws RocksDBException, IOException {
        if (n <= 0) n = 1 - n;

        Sha256Hash key = intToHash(n);
        NetworkParameters np = new MainNetParams();
        TransactionOutPoint top = new TransactionOutPoint(np, n, key);

        tof.put(key, top);
        assertThat(tof.get(key)).isEqualTo(List.of(top));
    }

    @ParameterizedTest
    @MethodSource("provideInts")
    void multipleTransactionOutpointFilter(Integer n) throws RocksDBException {
        if (n <= 0 || n > 100) n = 1 - (n % 100);

        Sha256Hash key = intToHash(n);
        NetworkParameters np = new MainNetParams();

        List<TransactionOutPoint> tops = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            TransactionOutPoint top = new TransactionOutPoint(np, n, key);
            tof.put(key, top);
            tops.add(top);
        }

        assertThat(tof.get(key)).isEqualTo(tops);
    }

    @ParameterizedTest
    @MethodSource("provideIntList")
    void multiValuedMapTransactionOutpointFilterComparison(List<Integer> ln) throws RocksDBException, IOException {
        MultiValuedMap<Sha256Hash, TransactionOutPoint> topMapping = new HashSetValuedHashMap<>();

        for (int i = 0; i < ln.size(); i++) {
            int n = ln.get(i);
            Sha256Hash key = intToHash(n);
            NetworkParameters np = new MainNetParams();
            TransactionOutPoint top = new TransactionOutPoint(np, n + i, key);

            tof.put(key, top);
            topMapping.put(key, top);
        }

        for (Integer n : ln) {
            Sha256Hash key = intToHash(n);
            assertThat(tof.get(key)).containsExactlyInAnyOrderElementsOf(topMapping.get(key));
        }

    }

    @AfterEach
    void cleanup() throws RocksDBException {
        for (ColumnFamilyHandle column : columns) {
            RocksIterator it = db.newIterator(column);
            it.seekToFirst();

            while (it.isValid()) {
                db.delete(column, it.key());
                it.next();
            }
        }

        ac.count = 0;
    }

    @AfterAll
    static void teardown() {
        pl.close();
    }

    private static Sha256Hash intToHash(int n) {
        return Sha256Hash.wrap(Arrays.copyOf(Integer.toString(n).getBytes(), 256 / 8));
    }

    private static Stream<Long> provideLongs() {
        return Stream.of(4L, 5L, 6L, 7L, 1L, 0L, -4L, -100L, Long.MAX_VALUE, Long.MIN_VALUE);
    }

    private static Stream<Integer> provideInts() {
        return Stream.of(4, 5, 6, 7, 1, 0, -4, -100, Integer.MAX_VALUE, Integer.MIN_VALUE);
    }

    private static Stream<byte[]> provideByteArrays() {
        return Stream.of(
                new byte[] {0},
                new byte[] {4, 5, 6, 1},
                new byte[] {Byte.MAX_VALUE, Byte.MIN_VALUE},
                new byte[] {-1, -5, 26, -127}
        );
    }

    private static Stream<List<Integer>> provideIntList() {
        return Stream.of(
                List.of(1, 2, -3, 4, 5),
                List.of(),
                List.of(10),
                List.of(0, -5, 4, Integer.MIN_VALUE),
                List.of(Integer.MAX_VALUE, 1000, 2),
                List.of(1, -4, 3, 4, 4)
        );
    }

    private static Stream<List<Long>> provideLongList() {
        return Stream.of(
                List.of(1L, 2L, 3L, 4L, 5L),
                List.of(),
                List.of(10L),
                List.of(0L, 5L),
                List.of(Long.MAX_VALUE, 1000L, 2L),
                List.of(1L, 4L, 3L, 4L, 4L)
        );
    }

    private static Stream<List<List<Long>>> provideLongListLongListLongList() {
        return Stream.of(
                List.of(List.of(1L, 2L, 3L, 4L, 5L), List.of(6L, 7L, 8L, 9L, 10L), List.of(11L, 12L, 13L, 14L, 15L)),
                List.of(List.of(1L, 2L, 3L, 4L), List.of(), List.of(14L, 14L, 14L))
        );
    }

    private static Stream<byte[]> provideByteList() {
        return Stream.of(
                ByteConversion.longList2bytes(List.of()),
                ByteConversion.longList2bytes(List.of(1L, 2L, 3L, 4L, 5L, 3L, 4L, 5L)),
                ByteConversion.longList2bytes(List.of(0L, -5L, 0L, -5L, 0L, -5L, 0L, -5L)),
                ByteConversion.longList2bytes(List.of(-1000L, -1L, -1000L, -1L, -1000L, -1L, -1000L, -1L)),
                ByteConversion.longList2bytes(List.of(Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE,
                        Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE))
        );
    }

    private static Object extract(Object src, String fld) throws NoSuchFieldException, IllegalAccessException {
        Field f = src.getClass().getDeclaredField(fld);
        f.setAccessible(true);
        return f.get(src);
    }
}
