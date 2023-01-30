package it.unimi.dsi.law;

import org.rocksdb.*;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static it.unimi.dsi.law.Parameters.*;

public class RocksDBWrapper implements Closeable {
	private final List<ColumnFamilyHandle> columnFamilyHandleList;
	private final DBOptions options;
	private final RocksDB database;
	private final boolean readonly;

	private final WriteBatch writeBatch;
	private final long WB_LIMIT = 10_000;

	public RocksDBWrapper (boolean readonly, Path location) throws RocksDBException {
		RocksDB.loadLibrary();
		this.readonly = readonly;

		ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()
				.optimizeUniversalStyleCompaction()
				.setMergeOperator(new StringAppendOperator())
				.setMaxBytesForLevelBase(MAX_BYTES_FOR_LEVEL_BASE);

		final List<ColumnFamilyDescriptor> columnFamilyDescriptors = List.of(
				new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
				new ColumnFamilyDescriptor("inputs".getBytes(), columnFamilyOptions),
				new ColumnFamilyDescriptor("outputs".getBytes(), columnFamilyOptions)
		);

		this.columnFamilyHandleList = new ArrayList<>();

		this.options = new DBOptions()
				.setCreateIfMissing(true)
				.setCreateMissingColumnFamilies(true)
				.setDbWriteBufferSize(WRITE_BUFFER_SIZE)
				.setMaxTotalWalSize(MAX_TOTAL_WAL_SIZE)
				.setMaxBackgroundJobs(MAX_BACKGROUND_JOBS);

		if (this.readonly) {
			this.writeBatch = null;
			this.database = RocksDB.openReadOnly(this.options, location.toString(), columnFamilyDescriptors, this.columnFamilyHandleList);
		} else {
			this.writeBatch = new WriteBatch();
			this.database = RocksDB.open(this.options, location.toString(), columnFamilyDescriptors, this.columnFamilyHandleList);
		}
	}

	public void add (Column column, byte[] transaction, byte[] address) throws RocksDBException {
		ColumnFamilyHandle handle = this.columnFamilyHandleList.get(column.index);
		this.writeBatch.merge(handle, transaction, address);

		if (this.writeBatch.getDataSize() > this.WB_LIMIT) {
			this.commit();
		}
	}

	public byte[] get (Column column, byte[] transaction) throws RocksDBException {
		ColumnFamilyHandle handle = this.columnFamilyHandleList.get(column.index);
		return this.database.get(handle, transaction);
	}

	public RocksIterator iterator (Column column) {
		ColumnFamilyHandle handle = this.columnFamilyHandleList.get(column.index);
		return this.database.newIterator(handle);
	}

	public void commit () throws RocksDBException {
		this.database.write(new WriteOptions(), this.writeBatch);
		this.writeBatch.clear();
	}

	public void close () {
		for (final ColumnFamilyHandle columnFamilyHandle : this.columnFamilyHandleList) {
			columnFamilyHandle.close();
		}

		if (!this.readonly) {
			try {
				this.commit();
				this.database.syncWal();
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
		}

		this.database.close();
		this.options.close();
	}

	enum Column {
		INPUT(1),
		OUTPUT(2);

		private final int index;

		Column (int index) {
			this.index = index;
		}
	}
}
