package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.AddressConversion;
import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.webgraph.ImmutableGraph;
import org.assertj.core.util.Files;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.TransactionOutPoint;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.extractProperty;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PersistenceLayerUnitTest {
    @Mock TransactionOutPoint top;
    @Mock Address address;

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
        when(top.getHash()).thenReturn(Sha256Hash.ZERO_HASH);

        im.put(top, ll);
        assertThat(im.get(top)).containsExactlyElementsOf(ll);
    }

    @ParameterizedTest
    @MethodSource("provideLongList")
    void multiplePutIncompleteMapping(List<Long> ll) throws RocksDBException {
        when(top.getHash()).thenReturn(Sha256Hash.ZERO_HASH);

        List<Long> ll1 = ll.subList(0, ll.size() / 2);
        List<Long> ll2 = ll.subList(ll.size() / 2, ll.size());

        im.put(top, ll1);
        im.put(top, ll2);

        assertThat(im.get(top)).containsExactlyElementsOf(ll);
    }

    @AfterEach
    void cleanup() throws NoSuchFieldException, IllegalAccessException, RocksDBException {
        Field fc = IncompleteMappings.class.getDeclaredField("column");
        fc.setAccessible(true);
        ColumnFamilyHandle column = (ColumnFamilyHandle) fc.get(im);

        db.delete(column, Sha256Hash.ZERO_HASH.getBytes());

        fc = AddressConversion.class.getDeclaredField("column");
        fc.setAccessible(true);
        column = (ColumnFamilyHandle) fc.get(ac);

        db.delete(column, Sha256Hash.ZERO_HASH.getBytes());
    }

    @AfterAll
    static void teardown() {
        pl.close();
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
                List.of(Long.MAX_VALUE, 1000L, 2L)
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
