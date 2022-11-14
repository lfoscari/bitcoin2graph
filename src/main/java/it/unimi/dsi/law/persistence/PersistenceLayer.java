package it.unimi.dsi.law.persistence;

import it.unimi.dsi.law.Parameters;
import org.bitcoinj.core.TransactionOutPoint;
import org.rocksdb.*;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static it.unimi.dsi.law.Parameters.MAX_BYTES_FOR_LEVEL_BASE;

public class PersistenceLayer implements Closeable {
	private final String location;
	private final ColumnFamilyOptions columnOptions;
	private final DBOptions options;

	public final List<ColumnFamilyHandle> columnFamilyHandleList;
	public final List<ColumnFamilyDescriptor> columnFamilyDescriptors;
	public RocksDB db;

	public PersistenceLayer (String location) throws RocksDBException {
		this(location, false);
	}

	public PersistenceLayer (String location, boolean readonly) throws RocksDBException {
		RocksDB.loadLibrary();

		this.location = location;

		this.columnOptions = new ColumnFamilyOptions()
				.optimizeUniversalStyleCompaction()
				.setMaxBytesForLevelBase(MAX_BYTES_FOR_LEVEL_BASE)
				.setMergeOperator(new StringAppendOperator(""));

		this.columnFamilyDescriptors = Arrays.asList(
				new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, this.columnOptions),
				new ColumnFamilyDescriptor("incomplete-mappings".getBytes(), this.columnOptions),
				new ColumnFamilyDescriptor("transaction-outpoint-filter".getBytes(), this.columnOptions)
		);

		this.columnFamilyHandleList = new ArrayList<>();

		this.options = new DBOptions()
				.setCreateIfMissing(true)
				.setCreateMissingColumnFamilies(true)
				.setDbWriteBufferSize(Parameters.WRITE_BUFFER_SIZE)
				.setMaxTotalWalSize(Parameters.MAX_TOTAL_WAL_SIZE)
				.setMaxBackgroundJobs(Parameters.MAX_BACKGROUND_JOBS);

		if (readonly) {
			this.db = RocksDB.openReadOnly(this.options, location, this.columnFamilyDescriptors, this.columnFamilyHandleList);
		} else {
			this.db = RocksDB.open(this.options, location, this.columnFamilyDescriptors, this.columnFamilyHandleList);
		}
	}

	public List<ColumnFamilyHandle> getColumnFamilyHandleList () {
		return this.columnFamilyHandleList;
	}

	public void mergeWith (PersistenceLayer other) throws RocksDBException {
		try (WriteBatch wb = new WriteBatch()) {
			for (ColumnFamilyHandle column : other.columnFamilyHandleList) {
				RocksIterator rit = other.db.newIterator(column);
				rit.seekToFirst();

				while (rit.isValid()) {
					wb.merge(column, rit.key(), rit.value());
					rit.next();
				}
			}

			this.db.write(new WriteOptions(), wb);
		}
	}

	public void delete () {
		this.close();
		this.deleteDirectory(new File(this.location));
	}

	public void close () {
		this.columnFamilyHandleList.forEach(ColumnFamilyHandle::close);

		this.options.close();
		this.db.close();
		this.columnOptions.close();
	}

	private boolean deleteDirectory (File directoryToBeDeleted) {
		File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
				this.deleteDirectory(file);
            }
        }
		return directoryToBeDeleted.delete();
	}
}
