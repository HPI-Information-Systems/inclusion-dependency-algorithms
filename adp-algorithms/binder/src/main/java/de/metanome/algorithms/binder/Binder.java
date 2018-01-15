package de.metanome.algorithms.binder;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.*;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.binder.io.FileInputIterator;
import de.metanome.algorithms.binder.io.InputIterator;
import de.metanome.algorithms.binder.structures.Attribute;
import de.metanome.algorithms.binder.structures.AttributeCombination;
import de.metanome.algorithms.binder.structures.IntSingleLinkedList;
import de.metanome.algorithms.binder.structures.IntSingleLinkedList.ElementIterator;
import de.metanome.algorithms.binder.structures.Level;
import de.metanome.algorithms.binder.utils.CollectionUtils;
import de.metanome.algorithms.binder.utils.DatabaseUtils;
import de.metanome.algorithms.binder.utils.FileUtils;
import de.metanome.algorithms.binder.dao.DataAccessObject;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;

// Bucketing IND ExtractoR (BINDER)
class Binder {

	// candidates for configuration class
	boolean detectNary = false;
	boolean filterKeyForeignkeys = false;
	int maxNaryLevel = -1;
	int inputRowLimit = -1;
	int numBucketsPerColumn = 10; // Initial number of buckets per column
	int memoryCheckFrequency = 100; // Number of new, i.e., so far unseen values during bucketing that trigger a memory consumption check
	int maxMemoryUsagePercentage = 60; // The algorithm spills to disc if memory usage exceeds X% of available memory
	private long maxMemoryUsage;

	// temp folder
	private File tempFolder = null;
	String tempFolderPath = "BINDER_temp"; // TODO: Use Metanome temp file functionality here (interface TempFileAlgorithm)
	boolean cleanTemp = true;

	private Int2ObjectOpenHashMap<List<List<String>>> attribute2subBucketsCache = null;

	private Int2ObjectOpenHashMap<IntSingleLinkedList> dep2ref = null;

	// candidates for
	List<TableInputGenerator> tableInputGenerator = null;
	List<RelationalInputGenerator> fileInputGenerator = null;
	InclusionDependencyResultReceiver resultReceiver = null;
	DataAccessObject dao = null;
	String databaseName = null;
	String[] tableNames = null;

	private final TableInfoFactory tableInfoFactory;
	private List<TableInfo> tables;
	private int[] column2table;

	Binder() {
		tableInfoFactory = new TableInfoFactory();
	}

	@Override
	public String toString() {
		return "it worked";
	}

	String getAuthorName() {
		return "Maxi Fischer, Axel Stebner";
	}

	String getDescriptionText() {
		return "Divide and Conquer-based IND discovery";
	}

	public void execute() throws AlgorithmExecutionException {
		// Disable Logging (FastSet sometimes complains about skewed key distributions with lots of WARNINGs)
		//LoggingUtils.disableLogging();
		long startExecutionTime = System.currentTimeMillis();

		try {

			////////////////////////////////////////////////////////
			// Phase 0: Initialization (Collect basic statistics) //
			////////////////////////////////////////////////////////
			long unaryStatisticTime = System.currentTimeMillis();
			this.initialize();
			unaryStatisticTime = System.currentTimeMillis() - unaryStatisticTime;
			System.out.println(unaryStatisticTime);

			//////////////////////////////////////////////////////
			// Phase 1: Bucketing (Create and fill the buckets) //
			//////////////////////////////////////////////////////
			long unaryLoadTime = System.currentTimeMillis();
			BucketMetadata bucketMetadata = this.bucketize();
			unaryLoadTime = System.currentTimeMillis() - unaryLoadTime;
			System.out.println(unaryLoadTime);
			//////////////////////////////////////////////////////
			// Phase 2: Checking (Check INDs using the buckets) //
			//////////////////////////////////////////////////////
			long unaryCompareTime = System.currentTimeMillis();
			//this.checkViaHashing(bucketMetadata);
			//this.checkViaSorting(bucketMetadata);
			//this.checkViaTwoStageIndexAndBitSets(bucketMetadata);
			this.checkViaTwoStageIndexAndLists(bucketMetadata);
			unaryCompareTime = System.currentTimeMillis() - unaryCompareTime;
			System.out.println(unaryCompareTime);
			/////////////////////////////////////////////////////////
			// Phase 3: N-ary IND detection (Find INDs of size > 1 //
			/////////////////////////////////////////////////////////4
			Map<AttributeCombination, List<AttributeCombination>> naryDep2ref = null;
			if (this.detectNary && (this.maxNaryLevel > 1 || this.maxNaryLevel <= 0)) {
				naryDep2ref = this.detectNaryViaBucketing(bucketMetadata);
				//naryDep2ref = this.detectNaryViaSingleChecks();
			}
			System.out.println(naryDep2ref);

			//////////////////////////////////////////////////////
			// Phase 4: Output (Return and/or write the results //
			//////////////////////////////////////////////////////

			long outputTime = System.currentTimeMillis();
			this.output(naryDep2ref);
			outputTime = System.currentTimeMillis() - outputTime;
			System.out.println(outputTime);
			System.out.println(String.format("Total Time: %d", (System.currentTimeMillis()) - startExecutionTime));
		}
		catch (SQLException | IOException e) {
			e.printStackTrace();
			throw new AlgorithmExecutionException(e.getMessage());
		} finally {
			// Clean temp
			if (this.cleanTemp)
				FileUtils.cleanDirectory(this.tempFolder);
		}
	}
	
	private void initialize() throws InputGenerationException, SQLException, InputIterationException, AlgorithmConfigurationException {
		System.out.println("Initializing ...");

		// Ensure the presence of an input generator
		if ((this.tableInputGenerator == null) && (this.fileInputGenerator == null))
			throw new InputGenerationException("No input generator specified!");

		this.tables = tableInfoFactory
				.create(fileInputGenerator,
						tableInputGenerator);

		// Initialize temp folder
		this.tempFolder = new File(this.tempFolderPath + File.separator + "temp");
		
		// Clean temp if there are files from previous runs that may pollute this run
		FileUtils.cleanDirectory(this.tempFolder);
		
		// Initialize memory management
		long availableMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
		long maxMemoryUsage = (long)(availableMemory * (this.maxMemoryUsagePercentage / 100.0f));

		// Build an index that assigns the columns to their tables, because the n-ary detection can only group those attributes that belong to the same table and the foreign key detection also only groups attributes from different tables.
		int currentStartIndex = 0;
		int currentTableIndex = 0;
		for (TableInfo table: tables) {
			for (int i = currentStartIndex; i < (currentStartIndex + table.getColumnCount()); i++)
				this.column2table[i] = currentTableIndex;
			currentStartIndex += table.getColumnCount();
			currentTableIndex++;
		}
	}

	private List<String> getTotalColumnNames() {
		return tables.stream().map(TableInfo::getColumnNames).flatMap(List::stream).collect(Collectors.toList());
	}

	private List<String> getTotalTableNames() {
		return tables.stream().map(TableInfo::getTableName).collect(Collectors.toList());
	}

	private int getTotalColumnCount() {
		return tables.stream().mapToInt(TableInfo::getColumnCount).sum();
	}

	private BucketMetadata bucketize() throws InputGenerationException, InputIterationException, IOException, AlgorithmConfigurationException {
		System.out.print("Bucketizing ... ");

		// externalized methods from initialize()
		int[] spillCounts = new int[getTotalColumnCount()];
		for (int columnNumber = 0; columnNumber < getTotalColumnCount(); columnNumber++)
			spillCounts[columnNumber] = 0;

		// Initialize the counters that count the empty buckets per bucket level to identify sparse buckets and promising bucket levels for comparison
		int[] emptyBuckets = new int[this.numBucketsPerColumn];

		BitSet nullValueColumns = new BitSet(getTotalColumnCount());

		// Initialize aggregators to measure the size of the columns
		LongArrayList columnSizes = new LongArrayList(getTotalColumnCount());
		for (int column = 0; column < getTotalColumnCount(); column++)
			columnSizes.add(0);

		int startTableColumnIndex = 0;
		for (TableInfo table : tables) {
			// Initialize buckets
			List<List<Set<String>>> buckets = new ArrayList<>(table.getColumnCount());
			for (int columnNumber = 0; columnNumber < table.getColumnCount(); columnNumber++) {
				ArrayList<Set<String>> attributeBuckets = new ArrayList<>();
				for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++)
					attributeBuckets.add(new HashSet<>());
				buckets.add(attributeBuckets);
			}

			// Initialize value counters
			int numValuesSinceLastMemoryCheck = 0;
			int[] numValuesInColumn = new int[table.getColumnCount()];
			for (int columnNumber = 0; columnNumber < table.getColumnCount(); columnNumber++)
				numValuesInColumn[columnNumber] = 0;
			long availableMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
			long maxMemoryUsage = (long) (availableMemory * (this.maxMemoryUsagePercentage / 100.0f));

			// Load data
			InputIterator inputIterator = null;
			try {
				inputIterator = new FileInputIterator(table.selectInputGenerator(), this.inputRowLimit);

				while (inputIterator.next()) {
					for (int columnNumber = 0; columnNumber < table.getColumnCount(); columnNumber++) {
						String value = inputIterator.getValue(columnNumber);

						//value = new StringBuilder(value).reverse().toString(); // This is an optimization if urls with long, common prefixes are used to later improve the comparison values

						if (value == null) {
							nullValueColumns.set(startTableColumnIndex + columnNumber);
							continue;
						}

						// Bucketize
						int bucketNumber = this.calculateBucketFor(value);
						if (buckets.get(columnNumber).get(bucketNumber).add(value)) {
							numValuesInColumn[columnNumber] = numValuesInColumn[columnNumber] + 1;
							numValuesSinceLastMemoryCheck++;
						}

						// Occasionally check the memory consumption
						if (numValuesSinceLastMemoryCheck >= this.memoryCheckFrequency) {
							numValuesSinceLastMemoryCheck = 0;

							// Spill to disk if necessary
							while (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > maxMemoryUsage) {
								// Identify largest buffer
								int largestColumnNumber = 0;
								int largestColumnSize = numValuesInColumn[largestColumnNumber];
								for (int otherColumnNumber = 1; otherColumnNumber < table.getColumnCount(); otherColumnNumber++) {
									if (largestColumnSize < numValuesInColumn[otherColumnNumber]) {
										largestColumnNumber = otherColumnNumber;
										largestColumnSize = numValuesInColumn[otherColumnNumber];
									}
								}

								// Write buckets from largest column to disk and empty written buckets
								int globalLargestColumnIndex = startTableColumnIndex + largestColumnNumber;
								for (int largeBucketNumber = 0; largeBucketNumber < this.numBucketsPerColumn; largeBucketNumber++) {
									columnSizes = this.writeBucket(globalLargestColumnIndex, largeBucketNumber, -1, buckets.get(largestColumnNumber).get(largeBucketNumber), columnSizes);
									buckets.get(largestColumnNumber).set(largeBucketNumber, new HashSet<>());
								}
								numValuesInColumn[largestColumnNumber] = 0;

								spillCounts[globalLargestColumnIndex] = spillCounts[globalLargestColumnIndex] + 1;

								System.gc();
							}
						}
					}
				}
			} finally {
				FileUtils.close(inputIterator);
			}

			// Write buckets to disk
			for (int columnNumber = 0; columnNumber < table.getColumnCount(); columnNumber++) {
				int globalColumnIndex = startTableColumnIndex + columnNumber;
				if (spillCounts[globalColumnIndex] == 0) { // if a column was spilled to disk, we do not count empty buckets for this column, because the partitioning distributes the values evenly and hence all buckets should have been populated
					for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++) {
						Set<String> bucket = buckets.get(columnNumber).get(bucketNumber);
						if (bucket.size() != 0)
							columnSizes = this.writeBucket(globalColumnIndex, bucketNumber, -1, bucket, columnSizes);
						else
							emptyBuckets[bucketNumber] = emptyBuckets[bucketNumber] + 1;
					}
				} else {
					for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++) {
						Set<String> bucket = buckets.get(columnNumber).get(bucketNumber);
						if (bucket.size() != 0)
							columnSizes = this.writeBucket(globalColumnIndex, bucketNumber, -1, bucket, columnSizes);
					}
				}
			}
			startTableColumnIndex += table.getColumnCount();
		}
		
		// Calculate the bucket comparison order from the emptyBuckets to minimize the influence of sparse-attribute-issue

		int[] bucketComparisonOrder = this.calculateBucketComparisonOrder(emptyBuckets);
		return new BucketMetadata(bucketComparisonOrder, nullValueColumns, columnSizes);
	}
		
	private void checkViaHashing(BucketMetadata bucketMetadata) throws IOException {
		/////////////////////////////////////////////////////////
		// Phase 2.1: Pruning (Dismiss first candidates early) //
		/////////////////////////////////////////////////////////

		// Setup the initial INDs using the first buckets
		int[] refCounts = new int[getTotalColumnCount()];
		this.dep2ref = new Int2ObjectOpenHashMap<>(getTotalColumnCount());
		Int2ObjectOpenHashMap<Set<String>> column2bucket = new Int2ObjectOpenHashMap<>(getTotalColumnCount());
		
		for (int globalColumnIndex = 0; globalColumnIndex < getTotalColumnCount(); globalColumnIndex++) {
			refCounts[globalColumnIndex] = 0;
			this.dep2ref.put(globalColumnIndex, new IntSingleLinkedList());
		}
		for (int c1 = 0; c1 < getTotalColumnCount(); c1++) {
			for (int c2 = c1 + 1; c2 < getTotalColumnCount(); c2++) {
					this.dep2ref.get(c1).add(c2);
			}
		}
		
		for (int column = 0; column < getTotalColumnCount(); column++)
			if (!(this.dep2ref.containsKey(column)) && (refCounts[column] == 0))
				column2bucket.remove(column);
		
		//////////////////////////////////////////////////////////////
		// Phase 2.2: Validation (Successively check all candidates //
		//////////////////////////////////////////////////////////////

		// Iterate the buckets for all remaining INDs until the end is reached or no more INDs exist)
		BitSet activeAttributes = new BitSet(getTotalColumnCount());
		activeAttributes.set(0, getTotalColumnCount());
		for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++) {
			// Refine the current bucket level if it does not fit into memory at once
			int[] subBucketNumbers = this.refineBucketLevel(activeAttributes, 0, bucketNumber, bucketMetadata);
			for (int subBucketNumber : subBucketNumbers) {
				if (column2bucket.keySet().isEmpty())
					break;
				
				// Load next bucket level
				for (int globalColumnIndex : column2bucket.keySet())
					column2bucket.put(globalColumnIndex, this.readBucketAsSet(globalColumnIndex, bucketNumber, subBucketNumber)); // Reading buckets into Sets eliminates all duplicates within these buckets
				
				// Check INDs
				IntList deps = new IntArrayList(this.dep2ref.keySet());
				for (int dep : deps) {
					Set<String> depBucket = column2bucket.get(dep);
					
					ElementIterator refIterator = this.dep2ref.get(dep).elementIterator();
					while (refIterator.hasNext()) {
						int ref = refIterator.next();
						Set<String> refBucket = column2bucket.get(ref);
						
						if ((depBucket.size() > refBucket.size()) || (!refBucket.containsAll(depBucket))) {
							refCounts[ref] = refCounts[ref] - 1;
							refIterator.remove();
						}
					}
					
					if (this.dep2ref.get(dep).isEmpty())
						this.dep2ref.remove(dep);
				}
				
				for (int column = 0; column < getTotalColumnCount(); column++)
					if (!(this.dep2ref.containsKey(column)) && (refCounts[column] == 0))
						column2bucket.remove(column);
			}
		}
	}
	
	private void checkViaSorting(BucketMetadata bucketMetadata) throws IOException {
		/////////////////////////////////////
		// Phase 2: Pruning and Validation //
		/////////////////////////////////////
		
		// Setup the initial INDs
		Int2ObjectOpenHashMap<Attribute> attributeId2attributeObject = new Int2ObjectOpenHashMap<>(getTotalColumnCount());
		PriorityQueue<Attribute> attributeObjectQueue = new PriorityQueue<>(getTotalColumnCount());
		IntArrayList activeAttributes = new IntArrayList(getTotalColumnCount());

		int globalColumnIndex = 0;
		for (TableInfo table: tables) {
			for (int k = 0; k < table.getColumnCount(); k++) {
				Attribute attribute = new Attribute(globalColumnIndex, table.getColumnTypes());
				attributeId2attributeObject.put(globalColumnIndex, attribute);

				if (!attribute.isPruneable())
					activeAttributes.add(globalColumnIndex);
				globalColumnIndex += 1;
			}
		}
		
		// Validate INDs
		for (int bucketNumber : bucketMetadata.getBucketComparisonOrder()) {
			// Refine the current bucket level if it does not fit into memory at once
			int[] subBucketNumbers = this.refineBucketLevel(activeAttributes, bucketNumber, bucketMetadata);
			for (int subBucketNumber : subBucketNumbers) {
				//this.activeAttributesPerBucketLevel.add(activeAttributes.size());
				if (activeAttributes.isEmpty())
					break;

				// Load next bucket layer
				for (int globalColumnIndex2: activeAttributes) {
					Attribute attribute = attributeId2attributeObject.get(globalColumnIndex2);
					attribute.setValues(this.readBucketAsList(globalColumnIndex2, bucketNumber, subBucketNumber));

					if (attribute.isRunning())
						attributeObjectQueue.add(attribute);
				}

				// Validate INDs on current bucket layer
				IntArrayList topAttributes = new IntArrayList(getTotalColumnCount());
				while (!attributeObjectQueue.isEmpty()) {
					Attribute topAttribute = attributeObjectQueue.remove();
					topAttributes.add(topAttribute.getAttributeId());
					while ((!attributeObjectQueue.isEmpty()) && topAttribute.getCurrentValue().equals(attributeObjectQueue.peek().getCurrentValue()))
						topAttributes.add(attributeObjectQueue.remove().getAttributeId());

					for (int attribute : topAttributes) {
						attributeId2attributeObject.get(attribute).intersectReferenced(topAttributes, attributeId2attributeObject);
					}
					for (int attribute : topAttributes) {
						topAttribute = attributeId2attributeObject.get(attribute);
						topAttribute.nextValue();

						if (topAttribute.isPruneable()) {
							activeAttributes.rem(topAttribute.getAttributeId());
							continue;
						}

						if (topAttribute.isRunning())
							attributeObjectQueue.add(topAttribute);
					}

					topAttributes.clear();
				}
			}
		}
		
		// Format the results
		this.dep2ref = new Int2ObjectOpenHashMap<>(getTotalColumnCount());
		attributeId2attributeObject.values().stream().filter(attribute -> !attribute.getReferenced().isEmpty()).forEach(attribute -> this.dep2ref.put(attribute.getAttributeId(), new IntSingleLinkedList(attribute.getReferenced())));
	}
	
	private void checkViaTwoStageIndexAndBitSets(BucketMetadata bucketMetadata) throws IOException {
		/////////////////////////////////////////////////////////
		// Phase 2.1: Pruning (Dismiss first candidates early) //
		/////////////////////////////////////////////////////////
		
		// Setup the initial INDs
		BitSet allAttributes = new BitSet(getTotalColumnCount());
		allAttributes.set(0, getTotalColumnCount());
		
		Int2ObjectOpenHashMap<BitSet> attribute2Refs = new Int2ObjectOpenHashMap<>(getTotalColumnCount());
		for (int column = 0; column < getTotalColumnCount(); column++) {
			BitSet refs = (BitSet)allAttributes.clone();
			refs.clear(column);
			attribute2Refs.put(column, refs);
		}
		
		// Apply data type pruning
		BitSet strings = new BitSet(getTotalColumnCount());
		BitSet numerics = new BitSet(getTotalColumnCount());
		BitSet temporals = new BitSet(getTotalColumnCount());
		BitSet unknown = new BitSet(getTotalColumnCount());
		int globalColumnIndex = 0;
		for (TableInfo table: tables) {
			for (String columnType: table.getColumnTypes()) {
				if (DatabaseUtils.isString(columnType))
					strings.set(globalColumnIndex);
				else if (DatabaseUtils.isNumeric(columnType))
					numerics.set(globalColumnIndex);
				else if (DatabaseUtils.isTemporal(columnType))
					temporals.set(globalColumnIndex);
				else
					unknown.set(globalColumnIndex);
				globalColumnIndex++;
			}
		}
		this.prune(attribute2Refs, strings);
		this.prune(attribute2Refs, numerics);
		this.prune(attribute2Refs, temporals);
		this.prune(attribute2Refs, unknown);
		
		// Apply statistical pruning
		// TODO ...
		
		///////////////////////////////////////////////////////////////
		// Phase 2.2: Validation (Successively check all candidates) //
		///////////////////////////////////////////////////////////////
		
		// Iterate the buckets for all remaining INDs until the end is reached or no more INDs exist
		BitSet activeAttributes = (BitSet)allAttributes.clone();
		levelloop : for (int bucketNumber : bucketMetadata.getBucketComparisonOrder()) { // TODO: Externalize this code into a method and use return instead of break
			// Refine the current bucket level if it does not fit into memory at once
			int[] subBucketNumbers = this.refineBucketLevel(activeAttributes, 0, bucketNumber, bucketMetadata);
			for (int subBucketNumber : subBucketNumbers) {
				// Identify all currently active attributes
				activeAttributes = this.getActiveAttributesFromBitSets(activeAttributes, attribute2Refs);
				//this.activeAttributesPerBucketLevel.add(activeAttributes.cardinality());
				if (activeAttributes.isEmpty())
					break levelloop;
				
				// Load next bucket level as two stage index
				Int2ObjectOpenHashMap<List<String>> attribute2Bucket = new Int2ObjectOpenHashMap<>(getTotalColumnCount());
				Map<String, BitSet> invertedIndex = new HashMap<>();
				for (int attribute = activeAttributes.nextSetBit(0); attribute >= 0; attribute = activeAttributes.nextSetBit(attribute + 1)) {
					// Build the index
					List<String> bucket = this.readBucketAsList(attribute, bucketNumber, subBucketNumber);
					attribute2Bucket.put(attribute, bucket);
					// Build the inverted index
					for (String value : bucket) {
						if (!invertedIndex.containsKey(value))
							invertedIndex.put(value, new BitSet(getTotalColumnCount()));
						invertedIndex.get(value).set(attribute);
					}
				}
				
				// Check INDs
				for (int attribute = activeAttributes.nextSetBit(0); attribute >= 0; attribute = activeAttributes.nextSetBit(attribute + 1)) {
					for (String value : attribute2Bucket.get(attribute)) {
						// Break if the attribute does not reference any other attribute
						if (attribute2Refs.get(attribute).isEmpty())
							break;
						
						// Continue if the current value has already been handled
						if (!invertedIndex.containsKey(value))
							continue;
						
						// Prune using the group of attributes containing the current value
						BitSet sameValueGroup = invertedIndex.get(value);
						this.prune(attribute2Refs, sameValueGroup);
						
						// Remove the current value from the index as it has now been handled
						invertedIndex.remove(value);
					}
				}
			}
		}
		
		// Format the results
		this.dep2ref = new Int2ObjectOpenHashMap<>(getTotalColumnCount());
		for (int dep = 0; dep < getTotalColumnCount(); dep++) {
			if (attribute2Refs.get(dep).isEmpty())
				continue;
			
			IntSingleLinkedList refs = new IntSingleLinkedList();
			for (int ref = attribute2Refs.get(dep).nextSetBit(0); ref >= 0; ref = attribute2Refs.get(dep).nextSetBit(ref + 1))
				refs.add(ref);
			
			this.dep2ref.put(dep, refs);
		}
	}
	

	private void checkViaTwoStageIndexAndLists(BucketMetadata bucketMetadata) throws IOException {
		System.out.println("Checking ...");
		
		/////////////////////////////////////////////////////////
		// Phase 2.1: Pruning (Dismiss first candidates early) //
		/////////////////////////////////////////////////////////
		
		// Setup the initial INDs using type information
		IntArrayList strings = new IntArrayList(getTotalColumnCount() / 2);
		IntArrayList numerics = new IntArrayList(getTotalColumnCount() / 2);
		IntArrayList temporals = new IntArrayList();
		IntArrayList unknown = new IntArrayList();
		int globalColumnIndex = 0;
		for (TableInfo table: tables) {
			for (String columnType: table.getColumnTypes()) {
				if (DatabaseUtils.isString(columnType))
					strings.add(globalColumnIndex);
				else if (DatabaseUtils.isNumeric(columnType))
					numerics.add(globalColumnIndex);
				else if (DatabaseUtils.isTemporal(columnType))
					temporals.add(globalColumnIndex);
				else
					unknown.add(globalColumnIndex);
				globalColumnIndex++;
			}
		}
		
		// Empty attributes can directly be placed in the output as they are contained in everything else; no empty attribute needs to be checked
		FetchedCandidates fetchedCandidates = new FetchedCandidates(new Int2ObjectOpenHashMap<>(getTotalColumnCount()), new Int2ObjectOpenHashMap<>(getTotalColumnCount()));
		fetchedCandidates = this.fetchCandidates(strings, fetchedCandidates, bucketMetadata);
		fetchedCandidates = this.fetchCandidates(numerics, fetchedCandidates, bucketMetadata);
		fetchedCandidates = this.fetchCandidates(temporals, fetchedCandidates, bucketMetadata);
		fetchedCandidates = this.fetchCandidates(unknown, fetchedCandidates, bucketMetadata);
		
		///////////////////////////////////////////////////////////////
		// Phase 2.2: Validation (Successively check all candidates) //
		///////////////////////////////////////////////////////////////
		
		// The initially active attributes are all non-empty attributes
		BitSet activeAttributes = new BitSet(getTotalColumnCount());
		for (int column = 0; column < getTotalColumnCount(); column++)
			if (bucketMetadata.getColumnSizes().getLong(column) > 0)
				activeAttributes.set(column);
		
		// Iterate the buckets for all remaining INDs until the end is reached or no more INDs exist
		levelloop : for (int bucketNumber : bucketMetadata.getBucketComparisonOrder()) { // TODO: Externalize this code into a method and use return instead of break
			// Refine the current bucket level if it does not fit into memory at once
			int[] subBucketNumbers = this.refineBucketLevel(activeAttributes, 0, bucketNumber, bucketMetadata);
			for (int subBucketNumber : subBucketNumbers) {
				// Identify all currently active attributes
				activeAttributes = this.getActiveAttributesFromLists(activeAttributes, fetchedCandidates.getDep2refToCheck());
				//this.activeAttributesPerBucketLevel.add(activeAttributes.cardinality());
				if (activeAttributes.isEmpty())
					break levelloop;
				
				// Load next bucket level as two stage index
				Int2ObjectOpenHashMap<List<String>> attribute2Bucket = new Int2ObjectOpenHashMap<>(getTotalColumnCount());
				Map<String, IntArrayList> invertedIndex = new HashMap<>();
				for (int attribute = activeAttributes.nextSetBit(0); attribute >= 0; attribute = activeAttributes.nextSetBit(attribute + 1)) {
					// Build the index
					List<String> bucket = this.readBucketAsList(attribute, bucketNumber, subBucketNumber);
					attribute2Bucket.put(attribute, bucket);
					// Build the inverted index
					for (String value : bucket) {
						if (!invertedIndex.containsKey(value))
							invertedIndex.put(value, new IntArrayList(2));
						invertedIndex.get(value).add(attribute);
					}
				}
				
				// Check INDs
				for (int attribute = activeAttributes.nextSetBit(0); attribute >= 0; attribute = activeAttributes.nextSetBit(attribute + 1)) {
					for (String value : attribute2Bucket.get(attribute)) {
						// Break if the attribute does not reference any other attribute
						if (fetchedCandidates.getDep2refToCheck().get(attribute).isEmpty())
							break;
						
						// Continue if the current value has already been handled
						if (!invertedIndex.containsKey(value))
							continue;
						
						// Prune using the group of attributes containing the current value
						IntArrayList sameValueGroup = invertedIndex.get(value);
						this.prune(fetchedCandidates.getDep2refToCheck(), sameValueGroup);

						// Remove the current value from the index as it has now been handled
						invertedIndex.remove(value);
					}
				}
			}
		}
		
		// Remove deps that have no refs
		IntIterator depIterator = fetchedCandidates.getDep2refToCheck().keySet().iterator();
		while (depIterator.hasNext()) {
			if (fetchedCandidates.getDep2refToCheck().get(depIterator.nextInt()).isEmpty())
				depIterator.remove();
		}
		this.dep2ref = fetchedCandidates.getDep2refToCheck();
		this.dep2ref.putAll(fetchedCandidates.getDep2refFinal());
	}

	private FetchedCandidates fetchCandidates(IntArrayList columns, FetchedCandidates fetchedCandidates, BucketMetadata bucketMetadata) {
		IntArrayList nonEmptyColumns = new IntArrayList(columns.size());
		nonEmptyColumns.addAll(columns.stream().filter(column -> bucketMetadata.getColumnSizes().getLong(column) > 0).collect(Collectors.toList()));

		if (this.filterKeyForeignkeys) {
			for (int dep : columns) {
				// Empty columns are no foreign keys
				if (bucketMetadata.getColumnSizes().getLong(dep) == 0)
					continue;
				
				// Referenced columns must not have null values and must come from different tables
				IntArrayList seed = nonEmptyColumns.clone();
				IntListIterator iterator = seed.iterator();
				while (iterator.hasNext()) {
					int ref = iterator.nextInt();
					if ((column2table[dep] == column2table[ref]) || bucketMetadata.getNullValueColumns().get(ref))
						iterator.remove();
				}
				
				fetchedCandidates.setDep2refToCheck(dep, new IntSingleLinkedList(seed, dep));
			}
		}
		else {
			for (int dep : columns) {
				if (bucketMetadata.getColumnSizes().getLong(dep) == 0)
					fetchedCandidates.setDep2refFinal(dep, new IntSingleLinkedList(columns, dep));
				else
					fetchedCandidates.setDep2refToCheck(dep, new IntSingleLinkedList(nonEmptyColumns, dep));
			}
		}
		return fetchedCandidates;
	}
	
	private void prune(Int2ObjectOpenHashMap<BitSet> attribute2Refs, BitSet attributeGroup) {
		for (int attribute = attributeGroup.nextSetBit(0); attribute >= 0; attribute = attributeGroup.nextSetBit(attribute + 1))
			attribute2Refs.get(attribute).and(attributeGroup);
	}

	private void prune(Int2ObjectOpenHashMap<IntSingleLinkedList> attribute2Refs, IntArrayList attributeGroup) {
		for (int attribute : attributeGroup)
			attribute2Refs.get(attribute).retainAll(attributeGroup);
	}
	
	private BitSet getActiveAttributesFromBitSets(BitSet previouslyActiveAttributes, Int2ObjectOpenHashMap<BitSet> attribute2Refs) {
		BitSet activeAttributes = new BitSet(getTotalColumnCount());
		for (int attribute = previouslyActiveAttributes.nextSetBit(0); attribute >= 0; attribute = previouslyActiveAttributes.nextSetBit(attribute + 1)) {
			// All attributes referenced by this attribute are active
			activeAttributes.or(attribute2Refs.get(attribute));
			// This attribute is active if it references any other attribute
			if (!attribute2Refs.get(attribute).isEmpty())
				activeAttributes.set(attribute);
		}
		return activeAttributes;
	}
	
	private BitSet getActiveAttributesFromLists(BitSet previouslyActiveAttributes, Int2ObjectOpenHashMap<IntSingleLinkedList> attribute2Refs) {
		BitSet activeAttributes = new BitSet(getTotalColumnCount());
		for (int attribute = previouslyActiveAttributes.nextSetBit(0); attribute >= 0; attribute = previouslyActiveAttributes.nextSetBit(attribute + 1)) {
			// All attributes referenced by this attribute are active
			attribute2Refs.get(attribute).setOwnValuesIn(activeAttributes);
			// This attribute is active if it references any other attribute
			if (!attribute2Refs.get(attribute).isEmpty())
				activeAttributes.set(attribute);
		}
		return activeAttributes;
	}
	
	private int calculateBucketFor(String value) {
		return Math.abs(value.hashCode() % this.numBucketsPerColumn); // range partitioning
	}

	private int calculateBucketFor(String value, int bucketNumber, int numSubBuckets) {
		return ((Math.abs(value.hashCode() % (this.numBucketsPerColumn * numSubBuckets)) - bucketNumber) / this.numBucketsPerColumn); // range partitioning
	}
	
	private int[] calculateBucketComparisonOrder(int[] emptyBuckets) {
		List<Level> levels = new ArrayList<>(getTotalColumnCount());
		for (int level = 0; level < this.numBucketsPerColumn; level++)
			levels.add(new Level(level, emptyBuckets[level]));
		Collections.sort(levels);
		
		int[] bucketComparisonOrder = new int[this.numBucketsPerColumn];
		for (int rank = 0; rank < this.numBucketsPerColumn; rank++)
			bucketComparisonOrder[rank] = levels.get(rank).getNumber();
		return bucketComparisonOrder;
	}

	private LongArrayList writeBucket(int attributeNumber, int bucketNumber, int subBucketNumber, Collection<String> values, LongArrayList columnSizes) throws IOException {
		// Write the values
		String bucketFilePath = this.getBucketFilePath(attributeNumber, bucketNumber, subBucketNumber);
		this.writeToDisk(bucketFilePath, values);
		
		// Add the size of the written values to the size of the current attribute
		long size = columnSizes.getLong(attributeNumber);
		int overheadPerValueForIndexes = 64;
		for (String value : values)
			size = size + (long)(8 * (Math.ceil(64 + 2 * value.length() / 8))) + overheadPerValueForIndexes;
		columnSizes.set(attributeNumber, size);
		return columnSizes;
	}
	
	private void writeToDisk(String bucketFilePath, Collection<String> values) throws IOException {
		if ((values == null) || (values.isEmpty()))
			return;
		
		BufferedWriter writer = null;
		try {
			writer = FileUtils.buildFileWriter(bucketFilePath, true);
			for (String value : values) {
				writer.write(value);
				writer.newLine();
			}
			writer.flush();
		}
		finally {
			FileUtils.close(writer);
		}
	}
	
	private Set<String> readBucketAsSet(int attributeNumber, int bucketNumber, int subBucketNumber) throws IOException {
		if ((this.attribute2subBucketsCache != null) && (this.attribute2subBucketsCache.containsKey(attributeNumber)))
			return new HashSet<>(this.attribute2subBucketsCache.get(attributeNumber).get(subBucketNumber));
		
		Set<String> bucket = new HashSet<>(); // Reading buckets into Sets eliminates all duplicates within these buckets
		String bucketFilePath = this.getBucketFilePath(attributeNumber, bucketNumber, subBucketNumber);
		this.readFromDisk(bucketFilePath, bucket);
		return bucket;
	}

	private List<String> readBucketAsList(int attributeNumber, int bucketNumber, int subBucketNumber) throws IOException {
		if ((this.attribute2subBucketsCache != null) && (this.attribute2subBucketsCache.containsKey(attributeNumber)))
			return this.attribute2subBucketsCache.get(attributeNumber).get(subBucketNumber);
		
		List<String> bucket = new ArrayList<>(); // Reading buckets into Lists keeps duplicates within these buckets
		String bucketFilePath = this.getBucketFilePath(attributeNumber, bucketNumber, subBucketNumber);
		this.readFromDisk(bucketFilePath, bucket);
		return bucket;
	}

	private void readFromDisk(String bucketFilePath, Collection<String> values) throws IOException {
		File file = new File(bucketFilePath);
		if (!file.exists())
			return;
		
		BufferedReader reader = null;
		String value;
		try {
			reader = FileUtils.buildFileReader(bucketFilePath);
			while ((value = reader.readLine()) != null)
				values.add(value);
		}
		finally {
			FileUtils.close(reader);
		}
	}

	private BufferedReader getBucketReader(int attributeNumber, int bucketNumber, int subBucketNumber) throws IOException {
		String bucketFilePath = this.getBucketFilePath(attributeNumber, bucketNumber, subBucketNumber);
		
		File file = new File(bucketFilePath);
		if (!file.exists())
			return null;
		
		return FileUtils.buildFileReader(bucketFilePath);
	}
	
	private String getBucketFilePath(int attributeNumber, int bucketNumber, int subBucketNumber) {
		if (subBucketNumber >= 0)
			return this.tempFolder.getPath() + File.separator + attributeNumber + File.separator + bucketNumber + "_" + subBucketNumber;
		return this.tempFolder.getPath() + File.separator + attributeNumber + File.separator + bucketNumber;
	}
	
	private int[] refineBucketLevel(IntArrayList activeAttributes, int level, BucketMetadata bucketMetadata) throws IOException {
		BitSet activeAttributesBits = new BitSet(getTotalColumnCount());
		for (Integer IntConsumer : activeAttributes)
			activeAttributesBits.set(IntConsumer);
		return this.refineBucketLevel(activeAttributesBits, 0, level, bucketMetadata);
	}
	
	private int[] refineBucketLevel(BitSet activeAttributes, int attributeOffset, int level, BucketMetadata bucketMetadata) throws IOException { // The offset is used for n-ary INDs, because their buckets are placed behind the unary buckets on disk, which is important if the unary buckets have not been deleted before
		// Empty sub bucket cache, because it will be refilled in the following
		this.attribute2subBucketsCache = null;

		// Give a hint to the gc
		System.gc();
		
		// Measure the size of the level and find the attribute with the largest bucket
		int numAttributes = 0;
		long levelSize = 0;
		for (int attribute = activeAttributes.nextSetBit(0); attribute >= 0; attribute = activeAttributes.nextSetBit(attribute + 1)) {
			numAttributes++;
			int attributeIndex = attribute + attributeOffset;
			long bucketSize = bucketMetadata.getColumnSizes().getLong(attributeIndex) / this.numBucketsPerColumn;
			levelSize = levelSize + bucketSize;
		}
		
		// If there are no active attributes, no refinement is needed
		if (numAttributes == 0) {
			int[] subBucketNumbers = new int[1];
			subBucketNumbers[0] = -1;
			return subBucketNumbers;
		}
		
		// Define the number of sub buckets
		long maxBucketSize = this.maxMemoryUsage / numAttributes;
		int numSubBuckets = (int)(levelSize / this.maxMemoryUsage) + 1;

		int[] subBucketNumbers = new int[numSubBuckets];
		
		// If the current level fits into memory, no refinement is needed
		if (numSubBuckets == 1) {
			subBucketNumbers[0] = -1;
			return subBucketNumbers;
		}
		
		for (int subBucketNumber = 0; subBucketNumber < numSubBuckets; subBucketNumber++)
			subBucketNumbers[subBucketNumber] = subBucketNumber;
		
		this.attribute2subBucketsCache = new Int2ObjectOpenHashMap<>(numSubBuckets);
		
		// Refine
		for (int attribute = activeAttributes.nextSetBit(0); attribute >= 0; attribute = activeAttributes.nextSetBit(attribute + 1)) {
			int attributeIndex = attribute + attributeOffset;
			
			List<List<String>> subBuckets = new ArrayList<>(numSubBuckets);
			//int expectedNewBucketSize = (int)(bucket.size() * (1.2f / numSubBuckets)); // The expected size is bucket.size()/subBuckets and we add 20% to it to avoid resizing
			//int expectedNewBucketSize = (int)(this.columnSizes.getLong(attributeIndex) / this.numBucketsPerColumn / 80); // We estimate an average String size of 8 chars, hence 64+2*8=80 byte
			for (int subBucket = 0; subBucket < numSubBuckets; subBucket++)
				subBuckets.add(new ArrayList<>());
			
			BufferedReader reader = null;
			String value;
			boolean spilled = false;
			try {
				reader = this.getBucketReader(attributeIndex, level, -1);
				
				if (reader != null) {
					int numValuesSinceLastMemoryCheck = 0;
					
					while ((value = reader.readLine()) != null) {
						int bucketNumber = this.calculateBucketFor(value, level, numSubBuckets);
						subBuckets.get(bucketNumber).add(value);
						numValuesSinceLastMemoryCheck++;
						
						// Occasionally check the memory consumption
						if (numValuesSinceLastMemoryCheck >= this.memoryCheckFrequency) {
							numValuesSinceLastMemoryCheck = 0;
							
							// Spill to disk if necessary
							if (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > this.maxMemoryUsage) {								
								for (int subBucket = 0; subBucket < numSubBuckets; subBucket++) {
									bucketMetadata.setColumnSizes(this.writeBucket(attributeIndex, level, subBucket, subBuckets.get(subBucket), bucketMetadata.getColumnSizes()));
									subBuckets.set(subBucket, new ArrayList<>());
								}
								
								spilled = true;
								System.gc();
							}
						}
					}
				}
			}
			finally {
				FileUtils.close(reader);
			}
			
			// Large sub bucketings need to be written to disk; small sub bucketings can stay in memory
			if ((bucketMetadata.getColumnSizes().getLong(attributeIndex) / this.numBucketsPerColumn > maxBucketSize) || spilled)
				for (int subBucket = 0; subBucket < numSubBuckets; subBucket++)
					bucketMetadata.setColumnSizes(this.writeBucket(attributeIndex, level, subBucket, subBuckets.get(subBucket), bucketMetadata.getColumnSizes()));
			else
				this.attribute2subBucketsCache.put(attributeIndex, subBuckets);
		}
		
		return subBucketNumbers;
	}

	private Map<AttributeCombination, List<AttributeCombination>> detectNaryViaBucketing(BucketMetadata bucketMetadata) throws InputGenerationException, InputIterationException, IOException, AlgorithmConfigurationException {
		System.out.print("N-ary IND detection ...");
		
		// Clean temp
		if (this.cleanTemp)
			FileUtils.cleanDirectory(this.tempFolder);
		
		// N-ary column combinations are enumerated following the enumeration of the attributes
		int naryOffset = getTotalColumnCount();

		// Initialize nPlusOneAryDep2ref with unary dep2ref
		Map<AttributeCombination, List<AttributeCombination>> nPlusOneAryDep2ref = new HashMap<>();
		for (int dep : this.dep2ref.keySet()) {
			AttributeCombination depAttributeCombination = new AttributeCombination(column2table[dep], dep);
			List<AttributeCombination> refAttributeCombinations = new LinkedList<>();

			ElementIterator refIterator = this.dep2ref.get(dep).elementIterator();
			while (refIterator.hasNext()) {
				int ref = refIterator.next();
				refAttributeCombinations.add(new AttributeCombination(column2table[ref], ref));
			}
			nPlusOneAryDep2ref.put(depAttributeCombination, refAttributeCombinations);
		}

		int naryLevel = 1;
		
		// Generate, bucketize and test the n-ary INDs level-wise
		LongArrayList naryGenerationTime = new LongArrayList();
		LongArrayList naryCompareTime = new LongArrayList();
		while (++naryLevel <= this.maxNaryLevel || this.maxNaryLevel <= 0) {
			System.out.print(" L" + naryLevel);
			
			// Generate (n+1)-ary IND candidates from the already identified unary and n-ary IND candidates
			final long naryGenerationTimeCurrent = System.currentTimeMillis();
			Map<AttributeCombination, List<AttributeCombination>> naryDep2ref = nPlusOneAryDep2ref;

			nPlusOneAryDep2ref = this.generateNPlusOneAryCandidates(nPlusOneAryDep2ref, bucketMetadata.getColumnSizes());
			if (nPlusOneAryDep2ref.isEmpty()) {
				nPlusOneAryDep2ref = naryDep2ref;
				naryLevel -= 1;
				break;
			}
			// Collect all attribute combinations of the current level that are possible refs or deps and enumerate them
			Set<AttributeCombination> attributeCombinationSet = new HashSet<>();
			attributeCombinationSet.addAll(nPlusOneAryDep2ref.keySet());
			nPlusOneAryDep2ref.values().forEach(attributeCombinationSet::addAll);
			List<AttributeCombination> attributeCombinations = new ArrayList<>(attributeCombinationSet);
			
			// Extend the columnSize array
			LongArrayList columnSizes = bucketMetadata.getColumnSizes();
			for (int i = 0; i < attributeCombinations.size(); i++)
				columnSizes.add(0);
			bucketMetadata.setColumnSizes(columnSizes);
			
			int[] currentNarySpillCounts = new int[attributeCombinations.size()];
			for (int attributeCombinationNumber = 0; attributeCombinationNumber < attributeCombinations.size(); attributeCombinationNumber++)
				currentNarySpillCounts[attributeCombinationNumber] = 0;
			
			naryGenerationTime.add(System.currentTimeMillis() - naryGenerationTimeCurrent);

			// Read the input dataset again and bucketize all attribute combinations that are refs or deps
			int[] bucketComparisonOrder = this.naryBucketize(attributeCombinations, naryOffset, currentNarySpillCounts, bucketMetadata);
			bucketMetadata.setBucketComparisonOrder(bucketComparisonOrder);
			// Check the n-ary IND candidates
			long naryCompareTimeCurrent = System.currentTimeMillis();
			nPlusOneAryDep2ref = this.naryCheckViaTwoStageIndexAndLists(nPlusOneAryDep2ref, attributeCombinations, naryOffset, bucketMetadata);

			// Add the number of created buckets for n-ary INDs of this level to the naryOffset
			naryOffset = naryOffset + attributeCombinations.size();

			naryCompareTime.add(System.currentTimeMillis() - naryCompareTimeCurrent);
			System.out.print("(" + (System.currentTimeMillis() - naryGenerationTimeCurrent) + " ms)");
		}

		return nPlusOneAryDep2ref;
	}
	
//	private Map<AttributeCombination, List<AttributeCombination>> detectNaryViaSingleChecks() throws InputGenerationException, AlgorithmConfigurationException {
//		if (this.tableInputGenerator == null)
//			throw new InputGenerationException("n-ary IND detection using De Marchi's MIND algorithm only possible on databases");
//
//		// Clean temp
//		if (this.cleanTemp)
//			FileUtils.cleanDirectory(this.tempFolder);
//
//		// Initialize nPlusOneAryDep2ref with unary dep2ref
//		Map<AttributeCombination, List<AttributeCombination>> nPlusOneAryDep2ref = new HashMap<>();
//		for (int dep : this.dep2ref.keySet()) {
//			AttributeCombination depAttributeCombination = new AttributeCombination(this.column2table[dep], dep);
//			List<AttributeCombination> refAttributeCombinations = new LinkedList<>();
//
//			ElementIterator refIterator = this.dep2ref.get(dep).elementIterator();
//			while (refIterator.hasNext()) {
//				int ref = refIterator.next();
//				refAttributeCombinations.add(new AttributeCombination(this.column2table[ref], ref));
//			}
//			nPlusOneAryDep2ref.put(depAttributeCombination, refAttributeCombinations);
//		}
//
//		// Generate, bucketize and test the n-ary INDs level-wise
//		Map<AttributeCombination, List<AttributeCombination>> naryDep2ref = new HashMap<>();
//		LongArrayList naryGenerationTime = new LongArrayList();
//		LongArrayList naryCompareTime = new LongArrayList();
//		while (true) {
//			// Generate (n+1)-ary IND candidates from the already identified unary and n-ary IND candidates
//			long naryGenerationTimeCurrent = System.currentTimeMillis();
//			nPlusOneAryDep2ref = this.generateNPlusOneAryCandidates(nPlusOneAryDep2ref);
//			if (nPlusOneAryDep2ref.isEmpty())
//				break;
//			naryGenerationTime.add(System.currentTimeMillis() - naryGenerationTimeCurrent);
//
//			// Check the n-ary IND candidates
//			long naryCompareTimeCurrent = System.currentTimeMillis();
//
//			Iterator<AttributeCombination> depIterator = nPlusOneAryDep2ref.keySet().iterator();
//			while (depIterator.hasNext()) {
//				AttributeCombination dep = depIterator.next();
//
//				List<AttributeCombination> refs = nPlusOneAryDep2ref.get(dep);
//
//				Iterator<AttributeCombination> refIterator = refs.iterator();
//				while (refIterator.hasNext()) {
//					AttributeCombination ref = refIterator.next();
//
//					String depTableName = this.tableNames[dep.getTable()];
//					String[] depAttributeNames = dep.getAttributes(this.columnNames);
//					String refTableName = this.tableNames[ref.getTable()];
//					String[] refAttributeNames = ref.getAttributes(this.columnNames);
//
//					String query = this.dao.buildSelectColumnCombinationNotInColumnCombinationQuery(depTableName, depAttributeNames, refTableName, refAttributeNames, 2);
//
//					ResultSet resultSet = null;
//					try {
//						resultSet = this.tableInputGenerator.generateResultSetFromSql(query);
//
//						// Check if there is a non-NULL value in the dep attribute combination
//						if (resultSet.next())
//							if ((resultSet.getString(1) != null) || resultSet.next())
//								refIterator.remove();
//					}
//					catch (InputGenerationException e) {
//						e.getCause().printStackTrace();
//						throw new InputGenerationException(e.getMessage() + "\nThe failed query was:\n" + query, e);
//					}
//					catch (SQLException e) {
//						e.printStackTrace();
//						throw new InputGenerationException(e.getMessage() + "\nThe failed query was:\n" + query, e);
//					}
//					finally {
//						try {
//							if (resultSet != null) {
//								Statement statement = resultSet.getStatement();
//								DatabaseUtils.close(resultSet);
//								DatabaseUtils.close(statement);
//							}
//						}
//						catch (SQLException ignored) {
//						}
//					}
//				}
//
//				if (nPlusOneAryDep2ref.get(dep).isEmpty())
//					depIterator.remove();
//			}
//
//			naryDep2ref.putAll(nPlusOneAryDep2ref);
//
//			naryCompareTime.add(System.currentTimeMillis() - naryCompareTimeCurrent);
//		}
//		return nPlusOneAryDep2ref;
//	}
/**/
	private Map<AttributeCombination, List<AttributeCombination>> generateNPlusOneAryCandidates(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, LongArrayList columnSizes) {
		Map<AttributeCombination, List<AttributeCombination>> nPlusOneAryDep2ref = new HashMap<>();
		
		if ((naryDep2ref == null) || (naryDep2ref.isEmpty()))
			return nPlusOneAryDep2ref;
		
		int previousSize = naryDep2ref.keySet().iterator().next().size();
		
//if (previousSize >= 3)
//	return nPlusOneAryDep2ref;
//System.out.println("apriori-gen level: " + (previousSize + 1));

		List<AttributeCombination> deps = new ArrayList<>(naryDep2ref.keySet());
		for (int i = 0; i < deps.size() - 1; i++) {
			AttributeCombination depPivot = deps.get(i);
			for (int j = i + 1; j < deps.size(); j++) { // if INDs of the form AA<CD should be discovered as well, remove + 1
				AttributeCombination depExtension = deps.get(j);
				// Ensure same tables
				if (depPivot.getTable() != depExtension.getTable())
					continue;

				// Ensure same prefix
				if (this.differentPrefix(depPivot, depExtension))
					continue;
				
				int depPivotAttr = depPivot.getAttributes()[previousSize - 1];
				int depExtensionAttr = depExtension.getAttributes()[previousSize - 1];
				
				// Ensure non-empty attribute extension
				if ((previousSize == 1) && ((columnSizes.getLong(depPivotAttr) == 0) || (columnSizes.getLong(depExtensionAttr) == 0)))
					continue;

				for (AttributeCombination refPivot : naryDep2ref.get(depPivot)) {
					for (AttributeCombination refExtension : naryDep2ref.get(depExtension)) {
						
						// Ensure same tables
						if (refPivot.getTable() != refExtension.getTable())
							continue;

						// Ensure same prefix
						if (this.differentPrefix(refPivot, refExtension))
							continue;

						int refPivotAttr = refPivot.getAttributes()[previousSize - 1];
						int refExtensionAttr = refExtension.getAttributes()[previousSize - 1];
						
						// Ensure that the extension attribute is different from the pivot attribute; remove check if INDs of the form AB<CC should be discovered as well
						if (refPivotAttr == refExtensionAttr)
							continue;

						// We want the lhs and rhs to be disjunct, because INDs with non-disjunct sides usually don't have practical relevance; remove this check if INDs with overlapping sides are of interest
						if ((depPivotAttr == refExtensionAttr) || (depExtensionAttr == refPivotAttr))
							continue;
						//if (nPlusOneDep.contains(nPlusOneRef.getAttributes()[previousSize - 1]) ||
						//	nPlusOneRef.contains(nPlusOneDep.getAttributes()[previousSize - 1]))
						//	continue;
						
						// The new candidate was created with two lhs and their rhs that share the same prefix; but other subsets of the lhs and rhs must also exist if the new candidate is larger than two attributes
						// TODO: Test if the other subsets exist as well (because this test is expensive, same prefix of two INDs might be a strong enough filter for now)

						// Merge the dep attributes and ref attributes, respectively
						AttributeCombination nPlusOneDep = new AttributeCombination(depPivot.getTable(), depPivot.getAttributes(), depExtensionAttr);
						AttributeCombination nPlusOneRef = new AttributeCombination(refPivot.getTable(), refPivot.getAttributes(), refExtensionAttr);
						
						// Store the new candidate
						if (!nPlusOneAryDep2ref.containsKey(nPlusOneDep))
							nPlusOneAryDep2ref.put(nPlusOneDep, new LinkedList<>());
						nPlusOneAryDep2ref.get(nPlusOneDep).add(nPlusOneRef);
						
//System.out.println(CollectionUtils.concat(nPlusOneDep.getAttributes(), ",") + "c" + CollectionUtils.concat(nPlusOneRef.getAttributes(), ","));
					}
				}
			}
		}
		return nPlusOneAryDep2ref;
	}

	private boolean differentPrefix(AttributeCombination combination1, AttributeCombination combination2) {
		for (int i = 0; i < combination1.size() - 1; i++)
			if (combination1.getAttributes()[i] != combination2.getAttributes()[i])
				return true;
		return false;
	}
/**//*
	private Map<AttributeCombination, List<AttributeCombination>> generateNPlusOneAryCandidates(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref) {
		Map<AttributeCombination, List<AttributeCombination>> nPlusOneAryDep2ref = new HashMap<AttributeCombination, List<AttributeCombination>>();
		
		if ((naryDep2ref == null) || (naryDep2ref.isEmpty()))
			return nPlusOneAryDep2ref;
		
//System.out.println("Level: " + (naryDep2ref.keySet().iterator().next().getAttributes().length + 1));
//if (naryDep2ref.keySet().iterator().next().getAttributes().length >= 3)
//	return nPlusOneAryDep2ref;
		
		for (AttributeCombination depAttributeCombination : naryDep2ref.keySet()) {
			if (!this.validAttributeCombinationForNaryCandidates(depAttributeCombination))
				continue;
			
			for (int dep : this.dep2ref.keySet()) {
				if (!this.validAttributeForNaryCandidates(dep))
					continue;
				
				if (!this.isCombineable(depAttributeCombination, dep))
					continue;
				
				AttributeCombination nPlusOneDep = new AttributeCombination(this.column2table[dep], depAttributeCombination.getAttributes(), dep);
				
				for (AttributeCombination refAttributeCombination : naryDep2ref.get(depAttributeCombination)) {
					if (!this.validAttributeCombinationForNaryCandidates(refAttributeCombination))
						continue;
					
					// The chosen extension of the dependent attribute group must not be included in the referenced attribute group
					if (refAttributeCombination.contains(dep))
						continue;
					
					ElementIterator refIterator = this.dep2ref.get(dep).elementIterator();
					while (refIterator.hasNext()) {
						int ref = refIterator.next();
						
						if (!this.validAttributeForNaryCandidates(ref))
							continue;
						
						if (!this.isCombineable(depAttributeCombination, refAttributeCombination, ref))
							continue;
						
						// If we reach here, we found a new n-ary IND candidate!
						AttributeCombination nPlusOneRef = new AttributeCombination(this.column2table[ref], refAttributeCombination.getAttributes(), ref);
						
						if (!nPlusOneAryDep2ref.containsKey(nPlusOneDep))
							nPlusOneAryDep2ref.put(nPlusOneDep, new LinkedList<AttributeCombination>());
						
						nPlusOneAryDep2ref.get(nPlusOneDep).add(nPlusOneRef);
						
//System.out.println(CollectionUtils.concat(nPlusOneDep.getAttributes(), ",") + "c" + CollectionUtils.concat(nPlusOneRef.getAttributes(), ","));
					}
				}
			}
		}
		return nPlusOneAryDep2ref;
	}
/**/
	private boolean validAttributeForNaryCandidates(int attribute, LongArrayList columnSizes, List<String> columnTypes) {
		// Do not use empty attributes
		if (columnSizes.getLong(attribute) == 0)
			return false;
		
		// Do not use CLOB or BLOB types; BINDER can handle this, but MIND cannot due to the use of SQL-join-checks
		return !DatabaseUtils.isLargeObject(columnTypes.get(attribute));

	}
	
	private boolean validAttributeCombinationForNaryCandidates(AttributeCombination attributeCombination, LongArrayList columnSizes, List<String> columnTypes) {
		// Attribute combinations of size > 1 are always valid for further extensions; their attributes have been checked before
		if (attributeCombination.getAttributes().length > 1)
			return true;
		
		int depInCombination = attributeCombination.getAttributes()[0];
		return this.validAttributeForNaryCandidates(depInCombination, columnSizes, columnTypes);
	}
	
//	private boolean isCombineable(AttributeCombination depAttributeCombination, int dep) {
//		// Do not combine attributes from different tables
//		if (depAttributeCombination.getTable() != this.column2table[dep])
//			return false;
//
//		// Do not use already contained or smaller attributes
//		for (int combinationAttribute : depAttributeCombination.getAttributes())
//			if (combinationAttribute >= dep)
//				return false;
//
//		return true;
//	}
//
//	private boolean isCombineable(AttributeCombination depAttributeCombination, AttributeCombination refAttributeCombination, int ref) {
//		// Do not combine attributes from different tables
//		if (refAttributeCombination.getTable() != this.column2table[ref])
//			return false;
//
//		// Do not use already contained attributes
//		return !(refAttributeCombination.contains(ref) || depAttributeCombination.contains(ref));
//
//	}

	private int[] naryBucketize(List<AttributeCombination> attributeCombinations, int naryOffset, int[] narySpillCounts, BucketMetadata bucketMetadata) throws InputGenerationException, InputIterationException, IOException, AlgorithmConfigurationException {
		// Identify the relevant attribute combinations for the different tables
		List<IntArrayList> table2attributeCombinationNumbers = new ArrayList<>(tables.size());
		table2attributeCombinationNumbers.addAll(tables.stream().map(ignored -> new IntArrayList()).collect(Collectors.toList()));

		for (int attributeCombinationNumber = 0; attributeCombinationNumber < attributeCombinations.size(); attributeCombinationNumber++){
		table2attributeCombinationNumbers.get(attributeCombinations.get(attributeCombinationNumber).getTable()).add(attributeCombinationNumber);
	}
		// Count the empty buckets per attribute to identify sparse buckets and promising bucket levels for comparison
		int[] emptyBuckets = new int[this.numBucketsPerColumn];
		for (int levelNumber = 0; levelNumber < this.numBucketsPerColumn; levelNumber++)
			emptyBuckets[levelNumber] = 0;

		int startTableColumnIndex = 0;
		for (int tableIndex = 0; tableIndex < tables.size(); tableIndex++) {
			int numTableAttributeCombinations = table2attributeCombinationNumbers.get(tableIndex).size();
			//int startTableColumnIndex = getTotalColumnCountList(tables).get(tableIndex);
			if (numTableAttributeCombinations == 0) {
				startTableColumnIndex += tables.get(tableIndex).getColumnCount();
				continue;
			}
			// Initialize buckets
			Int2ObjectOpenHashMap<List<Set<String>>> buckets = new Int2ObjectOpenHashMap<>(numTableAttributeCombinations);
			for (int attributeCombinationNumber : table2attributeCombinationNumbers.get(tableIndex)) {
				List<Set<String>> attributeCombinationBuckets = new ArrayList<>();
				for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++)
					attributeCombinationBuckets.add(new HashSet<>());
				buckets.put(attributeCombinationNumber, attributeCombinationBuckets);
			}

			// Initialize value counters
			int numValuesSinceLastMemoryCheck = 0;
			int[] numValuesInAttributeCombination = new int[attributeCombinations.size()];
			for (int attributeCombinationNumber = 0; attributeCombinationNumber < attributeCombinations.size(); attributeCombinationNumber++)
				numValuesInAttributeCombination[attributeCombinationNumber] = 0;
			long availableMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
			long maxMemoryUsage = (long)(availableMemory * (this.maxMemoryUsagePercentage / 100.0f));

			// Load data
			InputIterator inputIterator = null;
			try {
				inputIterator = new FileInputIterator(tables.get(tableIndex).selectInputGenerator(), this.inputRowLimit);
				
				while (inputIterator.next()) {
					List<String> values = inputIterator.getValues();
					for (int attributeCombinationNumber : table2attributeCombinationNumbers.get(tableIndex)) {

						AttributeCombination attributeCombination = attributeCombinations.get(attributeCombinationNumber);

						boolean anyNull = false;
						List<String> attributeCombinationValues = new ArrayList<>(attributeCombination.getAttributes().length);
						for (int attribute : attributeCombination.getAttributes()) {
							String attributeValue = values.get(attribute - startTableColumnIndex);
							if (anyNull = attributeValue == null) break;
							attributeCombinationValues.add(attributeValue);
						}
						if (anyNull) {
							startTableColumnIndex += tables.get(tableIndex).getColumnCount();
							continue;
						}
						String valueSeparator = "#";
						String value = CollectionUtils.concat(attributeCombinationValues, valueSeparator);
						
						// Bucketize
						int bucketNumber = this.calculateBucketFor(value);
						if (buckets.get(attributeCombinationNumber).get(bucketNumber).add(value)) {
							numValuesSinceLastMemoryCheck++;
							numValuesInAttributeCombination[attributeCombinationNumber] = numValuesInAttributeCombination[attributeCombinationNumber] + 1;
						}

						// Occasionally check the memory consumption
						if (numValuesSinceLastMemoryCheck >= this.memoryCheckFrequency) {
							numValuesSinceLastMemoryCheck = 0;
							
							// Spill to disk if necessary
							while (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > maxMemoryUsage) {
								// Identify largest buffer
								int largestAttributeCombinationNumber = 0;
								int largestAttributeCombinationSize = numValuesInAttributeCombination[largestAttributeCombinationNumber];
								for (int otherAttributeCombinationNumber = 1; otherAttributeCombinationNumber < numValuesInAttributeCombination.length; otherAttributeCombinationNumber++) {
									if (largestAttributeCombinationSize < numValuesInAttributeCombination[otherAttributeCombinationNumber]) {
										largestAttributeCombinationNumber = otherAttributeCombinationNumber;
										largestAttributeCombinationSize = numValuesInAttributeCombination[otherAttributeCombinationNumber];
									}
								}
								
								// Write buckets from largest column to disk and empty written buckets
								for (int largeBucketNumber = 0; largeBucketNumber < this.numBucketsPerColumn; largeBucketNumber++) {
									this.writeBucket(naryOffset + largestAttributeCombinationNumber, largeBucketNumber, -1, buckets.get(largestAttributeCombinationNumber).get(largeBucketNumber), bucketMetadata.getColumnSizes());
									buckets.get(largestAttributeCombinationNumber).set(largeBucketNumber, new HashSet<>());
								}
								
								numValuesInAttributeCombination[largestAttributeCombinationNumber] = 0;
								
								narySpillCounts[largestAttributeCombinationNumber] = narySpillCounts[largestAttributeCombinationNumber] + 1;
								
								System.gc();
							}
						}
					}
				}
			}
			finally {
				FileUtils.close(inputIterator);
			}
			
			// Write buckets to disk
			for (int attributeCombinationNumber : table2attributeCombinationNumbers.get(tableIndex)) {
				if (narySpillCounts[attributeCombinationNumber] == 0) { // if a attribute combination was spilled to disk, we do not count empty buckets for this attribute combination, because the partitioning distributes the values evenly and hence all buckets should have been populated
					for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++) {
						Set<String> bucket = buckets.get(attributeCombinationNumber).get(bucketNumber);
						if (bucket.size() != 0)
							this.writeBucket(naryOffset + attributeCombinationNumber, bucketNumber, -1, bucket, bucketMetadata.getColumnSizes());
						else
							emptyBuckets[bucketNumber] = emptyBuckets[bucketNumber] + 1;
					}
				}
				else {
					for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++) {
						Set<String> bucket = buckets.get(attributeCombinationNumber).get(bucketNumber);
						if (bucket.size() != 0)
							this.writeBucket(naryOffset + attributeCombinationNumber, bucketNumber, -1, bucket, bucketMetadata.getColumnSizes());
					}
				}
			}
			startTableColumnIndex += tables.get(tableIndex).getColumnCount();
		}
		
		// Calculate the bucket comparison order from the emptyBuckets to minimize the influence of sparse-attribute-issue
		int[] bucketComparisonOrder = this.calculateBucketComparisonOrder(emptyBuckets);
		return bucketComparisonOrder;
	}

	private Map<AttributeCombination, List<AttributeCombination>> naryCheckViaTwoStageIndexAndLists(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, List<AttributeCombination> attributeCombinations, int naryOffset, BucketMetadata bucketMetadata) throws IOException {
		////////////////////////////////////////////////////
		// Validation (Successively check all candidates) //
		////////////////////////////////////////////////////
		
		// Iterate the buckets for all remaining INDs until the end is reached or no more INDs exist
		BitSet activeAttributeCombinations = new BitSet(attributeCombinations.size());
		activeAttributeCombinations.set(0, attributeCombinations.size());
		levelloop : for (int bucketNumber : bucketMetadata.getBucketComparisonOrder()) { // TODO: Externalize this code into a method and use return instead of break
			// Refine the current bucket level if it does not fit into memory at once
			int[] subBucketNumbers = this.refineBucketLevel(activeAttributeCombinations, naryOffset, bucketNumber, bucketMetadata);
			for (int subBucketNumber : subBucketNumbers) {
				// Identify all currently active attributes
				activeAttributeCombinations = this.getActiveAttributeCombinations(activeAttributeCombinations, naryDep2ref, attributeCombinations);
				//this.naryActiveAttributesPerBucketLevel.add(activeAttributeCombinations.cardinality());
				if (activeAttributeCombinations.isEmpty())
					break levelloop;
				
				// Load next bucket level as two stage index
				Int2ObjectOpenHashMap<List<String>> attributeCombination2Bucket = new Int2ObjectOpenHashMap<>();
				Map<String, IntArrayList> invertedIndex = new HashMap<>();
				for (int attributeCombination = activeAttributeCombinations.nextSetBit(0); attributeCombination >= 0; attributeCombination = activeAttributeCombinations.nextSetBit(attributeCombination + 1)) {
					// Build the index
					List<String> bucket = this.readBucketAsList(naryOffset + attributeCombination, bucketNumber, subBucketNumber);
					attributeCombination2Bucket.put(attributeCombination, bucket);
					// Build the inverted index
					for (String value : bucket) {
						if (!invertedIndex.containsKey(value))
							invertedIndex.put(value, new IntArrayList(2));
						invertedIndex.get(value).add(attributeCombination);
					}
				}
				
				// Check INDs
				for (int attributeCombination = activeAttributeCombinations.nextSetBit(0); attributeCombination >= 0; attributeCombination = activeAttributeCombinations.nextSetBit(attributeCombination + 1)) {
					for (String value : attributeCombination2Bucket.get(attributeCombination)) {
						// Break if the attribute combination does not reference any other attribute combination
						if (!naryDep2ref.containsKey(attributeCombinations.get(attributeCombination)) || (naryDep2ref.get(attributeCombinations.get(attributeCombination)).isEmpty()))
							break;
						
						// Continue if the current value has already been handled
						if (!invertedIndex.containsKey(value))
							continue;
						
						// Prune using the group of attributes containing the current value
						IntArrayList sameValueGroup = invertedIndex.get(value);
						naryDep2ref = this.prune(naryDep2ref, sameValueGroup, attributeCombinations);
						
						// Remove the current value from the index as it has now been handled
						invertedIndex.remove(value);
					}
				}
			}
		}
		
		// Format the results
		Iterator<AttributeCombination> depIterator = naryDep2ref.keySet().iterator();
		while (depIterator.hasNext()) {
			if (naryDep2ref.get(depIterator.next()).isEmpty())
				depIterator.remove();
		}
		return naryDep2ref;
	}
	
	private BitSet getActiveAttributeCombinations(BitSet previouslyActiveAttributeCombinations, Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, List<AttributeCombination> attributeCombinations) {
		BitSet activeAttributeCombinations = new BitSet(attributeCombinations.size());
		for (int attribute = previouslyActiveAttributeCombinations.nextSetBit(0); attribute >= 0; attribute = previouslyActiveAttributeCombinations.nextSetBit(attribute + 1)) {
			AttributeCombination attributeCombination = attributeCombinations.get(attribute);
			if (naryDep2ref.containsKey(attributeCombination)) {
				// All attribute combinations referenced by this attribute are active
				for (AttributeCombination refAttributeCombination : naryDep2ref.get(attributeCombination))
					activeAttributeCombinations.set(attributeCombinations.indexOf(refAttributeCombination));
				// This attribute combination is active if it references any other attribute
				if (!naryDep2ref.get(attributeCombination).isEmpty())
					activeAttributeCombinations.set(attribute);
			}
		}
		return activeAttributeCombinations;
	}
	
	private Map<AttributeCombination, List<AttributeCombination>> prune(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, IntArrayList attributeCombinationGroupIndexes, List<AttributeCombination> attributeCombinations) {
		List<AttributeCombination> attributeCombinationGroup = new ArrayList<>(attributeCombinationGroupIndexes.size());
		attributeCombinationGroup.addAll(attributeCombinationGroupIndexes.stream().map(attributeCombinations::get).collect(Collectors.toList()));

		attributeCombinationGroup.stream().filter(naryDep2ref::containsKey).forEach(attributeCombination -> naryDep2ref.get(attributeCombination).retainAll(attributeCombinationGroup));
		return naryDep2ref;
	}
	
	private void output(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref) throws CouldNotReceiveResultException, ColumnNameMismatchException {
		System.out.println("Generating output ...");
		// Output unary INDs
		for (int dep : this.dep2ref.keySet()) {
			String depTableName = this.getTableNameFor(dep);
			String depColumnName = getTotalColumnNames().get(dep);
			
			ElementIterator refIterator = this.dep2ref.get(dep).elementIterator();

			while (refIterator.hasNext()) {
				int ref = refIterator.next();
				String refTableName = this.getTableNameFor(ref);
				String refColumnName = getTotalColumnNames().get(ref);

				System.out.print(depTableName + ": " + depColumnName + "\n");
				System.out.print(refTableName + ": " + refColumnName + "\n");
				this.resultReceiver.receiveResult(new InclusionDependency(new ColumnPermutation(new ColumnIdentifier(depTableName, depColumnName)), new ColumnPermutation(new ColumnIdentifier(refTableName, refColumnName))));
			}
		}
		// Output n-ary INDs
		if (naryDep2ref == null)
			return;
		for (AttributeCombination depAttributeCombination : naryDep2ref.keySet()) {
			ColumnPermutation dep = this.buildColumnPermutationFor(depAttributeCombination);
			System.out.println("Dep: " + dep);

			for (AttributeCombination refAttributeCombination : naryDep2ref.get(depAttributeCombination)) {
				ColumnPermutation ref = this.buildColumnPermutationFor(refAttributeCombination);
				System.out.println("Ref: " + ref);
				this.resultReceiver.receiveResult(new InclusionDependency(dep, ref));
			}
		}
	}
	
	private String getTableNameFor(int column) {
		int currentTableIndex = 0;
		for (TableInfo table: tables) {
			currentTableIndex += table.getColumnCount();
			if (column < currentTableIndex)
				return table.getTableName();
		}
		return "NULL";
	}
	
	private ColumnPermutation buildColumnPermutationFor(AttributeCombination attributeCombination) {
		String tableName = getTotalTableNames().get(attributeCombination.getTable());
		
		List<ColumnIdentifier> columnIdentifiers = new ArrayList<>(attributeCombination.getAttributes().length);
		for (int attributeIndex : attributeCombination.getAttributes())
			columnIdentifiers.add(new ColumnIdentifier(tableName, getTotalColumnNames().get(attributeIndex)));
		
		return new ColumnPermutation(columnIdentifiers.toArray(new ColumnIdentifier[0]));
	}
}