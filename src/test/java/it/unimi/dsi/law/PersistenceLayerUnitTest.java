package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.AddressConversion;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.TransactionOutPoint;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class PersistenceLayerUnitTest {
    @Mock
    TransactionOutPoint top;

    @Mock
    Address address;

    @ParameterizedTest
    @MethodSource("provideLongs")
    void convertFromLong(long l) {
        byte[] bb = AddressConversion.long2bytes(l);
        assertThat(AddressConversion.bytes2long(bb)).isEqualTo(l);
    }

    private static Stream<Long> provideLongs() {
        return Stream.of(4L, 5L, 6L, 7L, 1L, 0L, -4L, -100L, Long.MAX_VALUE, Long.MIN_VALUE);
    }

    @ParameterizedTest
    @MethodSource("provideByteArrays")
    void convertFromBytes(byte[] bb) {
        long l = AddressConversion.bytes2long(bb);
        assertThat(AddressConversion.trim(AddressConversion.long2bytes(l))).containsExactly(bb);
    }

    private static Stream<byte[]> provideByteArrays() {
        return Stream.of(
                new byte[] {0},
                new byte[] {4, 5, 6, 1},
                new byte[] {Byte.MAX_VALUE, Byte.MIN_VALUE},
                new byte[] {-1, -5, 26, -127}
        );
    }

    @ParameterizedTest
    @MethodSource("provideLongList")
    void convertFromLongList(List<Long> ll) {
        byte[] bb = AddressConversion.longList2bytes(ll);
        assertThat(AddressConversion.bytes2longList(bb)).isEqualTo(ll);
    }

    private static Stream<List<Long>> provideLongList() {
        return Stream.of(
                List.of(1L, 2L, 3L, 4L, 5L),
                List.of(0L, 5L),
                List.of(Long.MAX_VALUE, 1000L, 2L)
        );
    }

    @ParameterizedTest
    @MethodSource("provideByteList")
    void convertFromByteList(byte[] bb) {
        List<Long> ll = AddressConversion.bytes2longList(bb);
        assertThat(AddressConversion.longList2bytes(ll)).isEqualTo(bb);
    }

    private static Stream<byte[]> provideByteList() {
        return Stream.of(
                AddressConversion.longList2bytes(List.of(1L, 2L, 3L, 4L, 5L, 3L, 4L, 5L)),
                AddressConversion.longList2bytes(List.of(0L, -5L, 0L, -5L, 0L, -5L, 0L, -5L)),
                AddressConversion.longList2bytes(List.of(-1000L, -1L, -1000L, -1L, -1000L, -1L, -1000L, -1L)),
                AddressConversion.longList2bytes(List.of(Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE,
                        Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE))
        );
    }
}
