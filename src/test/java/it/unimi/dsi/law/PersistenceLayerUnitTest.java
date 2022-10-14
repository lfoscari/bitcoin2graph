package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.AddressConversion;
import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.assertj.core.util.Files;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.TransactionOutPoint;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.extractProperty;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PersistenceLayerUnitTest {
    @Mock TransactionOutPoint top_a, top_b, top_c;
    @Mock Address addr_a, addr_b, addr_c;

    static PersistenceLayer pl;
    static IncompleteMappings im;
    static AddressConversion ac;

    static RocksDB db;

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
    }

    @ParameterizedTest
    @MethodSource("provideLongs")
    void convertFromLong(long l) {
        byte[] bb = AddressConversion.long2bytes(l);
        assertThat(AddressConversion.bytes2long(bb)).isEqualTo(l);
    }

    @ParameterizedTest
    @MethodSource("provideByteArrays")
    void convertFromBytes(byte[] bb) {
        long l = AddressConversion.bytes2long(bb);
        assertThat(AddressConversion.trim(AddressConversion.long2bytes(l))).containsExactly(bb);
    }

    @ParameterizedTest
    @MethodSource("provideLongList")
    void convertFromLongList(List<Long> ll) {
        byte[] bb = AddressConversion.longList2bytes(ll);
        assertThat(AddressConversion.bytes2longList(bb)).isEqualTo(ll);
    }

    @ParameterizedTest
    @MethodSource("provideByteList")
    void convertFromByteList(byte[] bb) {
        List<Long> ll = AddressConversion.bytes2longList(bb);
        assertThat(AddressConversion.longList2bytes(ll)).isEqualTo(bb);
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
        assertThat(im.get(top_a)).isNullOrEmpty();

        List<Long> ll1 = ll.subList(0, ll.size() / 2);
        List<Long> ll2 = ll.subList(ll.size() / 2, ll.size());

        im.put(top_a, ll1);
        im.put(top_a, ll2);

        assertThat(im.get(top_a)).containsExactly(ll.toArray(new Long[0]));
    }

    @ParameterizedTest
    @MethodSource("provideLongList")
    void multipleEmptyPutIncompleteMapping(List<Long> ll) throws RocksDBException {
        assertThat(im.get(top_a)).isNullOrEmpty();

        im.put(top_a, ll);
        im.put(top_a, List.of());
        im.put(top_a, List.of());
        im.put(top_a, List.of());

        assertThat(im.get(top_a)).containsExactly(ll.toArray(new Long[0]));
    }

    @Test
    void consistencyAddressConversion() throws RocksDBException {
        when(addr_a.getHash()).thenReturn(intToHash(35).getBytes());
        when(addr_b.getHash()).thenReturn(intToHash(42).getBytes());

        long ai = ac.mapAddress(addr_a);
        long bi = ac.mapAddress(addr_b);

        assertThat(ac).extracting("count").isEqualTo(2L);
        assertThat(ai).isNotEqualTo(bi);
    }

    @Test
    void consistencySingleAddressConversion() throws RocksDBException {
        when(addr_a.getHash()).thenReturn(intToHash(35).getBytes());

        long ai = ac.mapAddress(addr_a);
        assertThat(ai).isEqualTo(ac.mapAddress(addr_a));
    }

    @Test
    void multipleAddressConversion() throws RocksDBException {
        when(addr_a.getHash()).thenReturn(intToHash(35).getBytes());

        long ai = ac.mapAddress(addr_a);
        assertThat(ai).isEqualTo(0L);
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

        assertThat(im.get(top_a)).containsExactly(ll.toArray(new Long[0]));
        assertThat(im.get(top_b)).containsExactly(ll.toArray(new Long[0]));
        assertThat(im.get(top_c)).containsExactly(ll.toArray(new Long[0]));
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

        assertThat(im.get(top_a)).containsExactly(lll.toArray(new Long[0]));
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

        System.out.println(im.get(top_a));
        System.out.println(mvm.get(top_a));

        assertThat(im.get(top_a)).containsExactlyInAnyOrder(mvm.get(top_a).toArray(new Long[0]));
        assertThat(im.get(top_b)).containsExactlyInAnyOrder(mvm.get(top_b).toArray(new Long[0]));
        assertThat(im.get(top_c)).containsExactlyInAnyOrder(mvm.get(top_c).toArray(new Long[0]));
    }

    @ParameterizedTest
    @MethodSource("provideLongList")
    void distinctLongList(List<Long> ll) throws RocksDBException {
        List<Long> ll_mut = new ArrayList<>(ll);
        ll_mut.add(null);

        im.put(top_a, ll_mut);
        assertThat(im.get(top_a).stream().filter(Objects::nonNull).distinct().toArray())
                .containsExactlyInAnyOrder(ll_mut.stream().filter(Objects::nonNull).distinct().toArray());
    }

    @AfterEach
    void cleanup() throws NoSuchFieldException, IllegalAccessException, RocksDBException {
        Field fc = IncompleteMappings.class.getDeclaredField("column");
        fc.setAccessible(true);
        ColumnFamilyHandle column = (ColumnFamilyHandle) fc.get(im);

        db.delete(column, intToHash(35).getBytes());
        db.delete(column, intToHash(42).getBytes());
        db.delete(column, intToHash(65).getBytes());

        fc = AddressConversion.class.getDeclaredField("column");
        fc.setAccessible(true);
        column = (ColumnFamilyHandle) fc.get(ac);

        db.delete(column, intToHash(35).getBytes());
        db.delete(column, intToHash(42).getBytes());
        db.delete(column, intToHash(65).getBytes());

        fc = AddressConversion.class.getDeclaredField("count");
        fc.setAccessible(true);
        fc.set(ac, 0L);
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

    private static Stream<byte[]> provideByteArrays() {
        return Stream.of(
                new byte[] {0},
                new byte[] {4, 5, 6, 1},
                new byte[] {Byte.MAX_VALUE, Byte.MIN_VALUE},
                new byte[] {-1, -5, 26, -127}
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
                AddressConversion.longList2bytes(List.of()),
                AddressConversion.longList2bytes(List.of(1L, 2L, 3L, 4L, 5L, 3L, 4L, 5L)),
                AddressConversion.longList2bytes(List.of(0L, -5L, 0L, -5L, 0L, -5L, 0L, -5L)),
                AddressConversion.longList2bytes(List.of(-1000L, -1L, -1000L, -1L, -1000L, -1L, -1000L, -1L)),
                AddressConversion.longList2bytes(List.of(Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE,
                        Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE))
        );
    }
}
