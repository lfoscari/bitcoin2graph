package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.law.utils.ByteConversion;
import org.rocksdb.*;

import java.nio.file.Path;

public class Address2Node {
    public static void saveAddress2Node(long[] ids, AddressConversion addressConversion) throws RocksDBException {
         RocksDB db = startDatabase();

        Long2IntOpenHashMap indices = new Long2IntOpenHashMap();
        for (int i = 0; i < ids.length; i++)
            indices.put(ids[i], i);

        save(db, indices, addressConversion);
        db.close();
    }

    private static void save(RocksDB db, Long2IntOpenHashMap indices, AddressConversion addressConversion) {
        try (WriteBatch wb = new WriteBatch(); RocksIterator rit = addressConversion.iterator()) {
            for (; rit.isValid(); rit.next()) {
                byte[] key = rit.key();
                byte[] value = rit.value();

                long count = ByteConversion.bytes2long(value);
                int index = indices.get(count);
                wb.put(key, ByteConversion.long2bytes(index));

                if (wb.getDataSize() > 10000) {
                    db.write(new WriteOptions(), wb);
                    wb.clear();
                }
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static RocksDB startDatabase() throws RocksDBException {
        RocksDB.loadLibrary();

        Path location = Path.of(Parameters.resources + "address2node");
        location.toFile().mkdir();

        Options options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setDbWriteBufferSize(Parameters.WRITE_BUFFER_SIZE)
                .setMaxTotalWalSize(Parameters.MAX_TOTAL_WAL_SIZE)
                .setMaxBackgroundJobs(Parameters.MAX_BACKGROUND_JOBS);

        return RocksDB.open(options, location.toString());
    }
}
