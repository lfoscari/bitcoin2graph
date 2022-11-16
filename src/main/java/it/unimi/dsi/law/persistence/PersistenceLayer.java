package it.unimi.dsi.law.persistence;

import it.unimi.dsi.law.Parameters;
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
	public final RocksDB db;

	public PersistenceLayer (String location) throws RocksDBException {
		RocksDB.loadLibrary();

		this.location = location;

		this.columnOptions = new ColumnFamilyOptions()
				.optimizeUniversalStyleCompaction()
				.setMaxBytesForLevelBase(MAX_BYTES_FOR_LEVEL_BASE)
				.setMergeOperator(new StringAppendOperator(""));

		this.columnFamilyDescriptors = Arrays.asList(
				new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, this.columnOptions),
				new ColumnFamilyDescriptor("incomplete-mappings".getBytes(), this.columnOptions),
				new ColumnFamilyDescriptor("transaction-addresses".getBytes(), this.columnOptions),
				new ColumnFamilyDescriptor("arcs".getBytes(), this.columnOptions)
		);

		this.columnFamilyHandleList = new ArrayList<>();

		this.options = new DBOptions()
				.setCreateIfMissing(true)
				.setCreateMissingColumnFamilies(true)
				.setDbWriteBufferSize(Parameters.WRITE_BUFFER_SIZE)
				.setMaxTotalWalSize(Parameters.MAX_TOTAL_WAL_SIZE)
				.setMaxBackgroundJobs(Parameters.MAX_BACKGROUND_JOBS);

		this.db = RocksDB.open(this.options, location, this.columnFamilyDescriptors, this.columnFamilyHandleList);
	}

	public List<ColumnFamilyHandle> getColumnFamilyHandleList () {
		return this.columnFamilyHandleList;
	}

	public RocksIterator iterator (ColumnFamilyHandle column) {
		RocksIterator rit = this.db.newIterator(column);
		rit.seekToFirst();

		return rit;
	}

	public boolean delete () {
		this.close();
		return this.deleteDirectory(new File(this.location));
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
