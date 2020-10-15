import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;

public class BucketSort {

    public ReentrantLock lock = new ReentrantLock();
    private ArrayList<Thread> threads = new ArrayList<>();
    public volatile ArrayList<Integer>[] buckets;

    // TODO: REFACTOR VARS
    public int N;
    public int K;
    public int M;

    public int[] sequentialBucketSort(int[] intArr, int max, int noOfBuckets) {
        long startTime = System.currentTimeMillis();
        System.out.print("\tCreating buckets: ");
        // Create bucket array
        ArrayList<Integer>[] buckets = new ArrayList[noOfBuckets];
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        startTime = System.currentTimeMillis();
        System.out.print("\tAssign a new ArrayList object to all indices of buckets: ");
        // Associate a list with each index
        // in the bucket array
        for (int i = 0; i < noOfBuckets; i++) {
            buckets[i] = new ArrayList<>();
        }
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        startTime = System.currentTimeMillis();
        System.out.print("\tAssigning numbers from array to the proper bucket by hash: ");
        // Assign numbers from array to the proper bucket
        // by using hashing function
        for (int num : intArr) {
            buckets[hashBucketIndex(num, max, noOfBuckets)].add(num);
        }
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        startTime = System.currentTimeMillis();
        System.out.print("\tSorting the individual buckets: ");
        // sort buckets
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = insertionSort(buckets[i]);
        }
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        startTime = System.currentTimeMillis();
        System.out.print("\tMerging the buckets to get sorted array: ");
        int i = 0;
        // Merge buckets to get sorted array
        for (List<Integer> bucket : buckets) {
            for (int num : bucket){
                intArr[i++] = num;
            }
        }
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        return intArr;
    }

    public void parallelBucketSort(int[] formattedInput, int max, int processors) {
        //TODO: refactor vars
        N = formattedInput.length;
        K = processors;
        M = K;

        long startTime = System.currentTimeMillis();
        System.out.print("\tInitializing buckets: ");

        // STEP 1
        initBuckets(M);

        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        threads = new ArrayList<>();

        startTime = System.currentTimeMillis();
        System.out.print("\tInitializing partitions: ");

        // STEP 2
        final double N_DIV_K = Math.floor((double) N / (double)K); // PARTITION SIZE OF N/K ELEMENTS
        ArrayList<int[]> partitions = initPartitions(formattedInput, K, N_DIV_K);

        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        startTime = System.currentTimeMillis();
        System.out.print("\tPartitioning buckets(parallel): ");

        // STEP 3 + 4
        runParallelPartitionBucketer(K, partitions, max);

        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

        // STEP 5
        startTime = System.currentTimeMillis();
        System.out.print("\tSorting partitions containing buckets(parallel): ");

        runBucketSortTasks(K, M);
        System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

    }

    public ArrayList<Integer> insertionSort(ArrayList<Integer> array) {
        int n = array.size();
        for (int j = 1; j < n; j++) {
            int key = array.get(j);
            int i = j-1;
            while ( (i > -1) && ( array.get(i)> key ) ) {
                array.set(i+1, array.get(i));
                i--;
            }
            array.set(i+1, key);
        }
        return array;
    }

    /**
     * \
     * @param bucketsM Size of initial collection of buckets at post partitioning stage
     */
    private void initBuckets(int bucketsM) {

        buckets = new ArrayList[bucketsM];
        for (int i = 0; i < bucketsM; i++) {
            buckets[i] = new ArrayList<>();
        }
    }

    private ArrayList<int[]> initPartitions(int[] formattedInput, int threadsK, double partitionLengthNdivK) {
        ArrayList<int[]> partitions = new ArrayList<>();

        int lengthCounter = 0;

        for (int i = 0; i < threadsK; i++) {

            int startIndex = i * (int) partitionLengthNdivK;
            int endIndex = (i * (int) partitionLengthNdivK) + (int) partitionLengthNdivK;

            int[] partition = null;

            if (i == threadsK-1 && lengthCounter < formattedInput.length) {
                partition = Arrays.copyOfRange(formattedInput, startIndex, formattedInput.length);
            } else {
                partition = Arrays.copyOfRange(formattedInput, startIndex, endIndex);
            }
            lengthCounter = lengthCounter + partition.length;
            partitions.add(partition);
        }

        return partitions;
    }

    private void runParallelPartitionBucketer(int threadsK, ArrayList<int[]> partitions, int max) {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(threadsK, () -> {
        });

        partitions.forEach(partition -> threads.add(new Thread( () -> {
            for (int i = 0; i < partition.length; i++) {
                // HASH FUNC FROM CUSTOM IMPLEMENTATION
                lock.lock();

                double targetIndex = BucketSort.hashBucketIndex(partition[i], max, buckets.length);

                if (targetIndex > buckets.length) {
                    System.out.println(targetIndex);
                }

                try{
                    buckets[(int) targetIndex].add(partition[i]);
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.out.println("index length " + targetIndex + "\t buckets length:" + buckets.length + " " + partition[i]);
                }
                lock.unlock();
            }
            try {
                cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }, "Partition-Thread-" + partitions.indexOf(partition))));

        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        threads.clear();
    }

    private void runBucketSortTasks(int threadsK, int bucketsM) {
        ArrayList<ArrayList<Integer>> bucketPartitions = new ArrayList<>();

        CyclicBarrier cyclicBarrierPartitions = new CyclicBarrier(threadsK, () -> {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("[");
            for (int i = 0; i < bucketPartitions.size(); i++) {
                boolean isEmpty = bucketPartitions.get(i).isEmpty();

                if (i == bucketPartitions.size() - 1 && !isEmpty) {
                    stringBuilder.append(Arrays.toString(bucketPartitions.get(i).toArray())
                            .replace("[","")
                            .replace("]", ""));
                } else if (!isEmpty){
                    stringBuilder.append(Arrays.toString(bucketPartitions.get(i).toArray())
                            .replace("[","")
                            .replace("]", ", "));
                }

            }
            stringBuilder.append("]");
            try {
                Files.writeString(Driver.OUTPUT_FILE_PARALLEL.toPath(), stringBuilder.toString() + "\n", StandardOpenOption.APPEND);
            }
            catch (IOException e) {
                System.out.println("Shit's whack, cannot write to file.");
            }
        });

        for (int i = 0; i < threadsK; i++) {
            int startIndex = i * bucketsM / threadsK;
            int endIndex = (i * bucketsM / threadsK) + bucketsM / threadsK;

            for (int j = startIndex; j < endIndex; j++) {
                bucketPartitions.add(i, buckets[j]);
            }

        }

        for (int i = 0; i < bucketPartitions.size(); i++) {
            int index = i;
            threads.add(new Thread( () -> {
                // STEP 6
                bucketPartitions.set(index,insertionSort(bucketPartitions.get(index)));

                try {
                    cyclicBarrierPartitions.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }, "Sort-Thread-" + i));
        }

        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Used to retrieve index of bucket an input value will be put in. In scenario of 24 threads, a max of 10000,
     * denominator is 10000 / 24 = 416. bucketIndex is retrieved by dividing the input value by this denominator.
     *
     * @param value
     * @param max
     * @param bucketLength
     * @return Index of the bucket an input value will be sorted in.
     */
    public static int hashBucketIndex(double value, double max, double bucketLength) {
        double bucketDenominator = max / bucketLength;
        int bucketIndex = (int) Math.floor(value / bucketDenominator);

        // If index (in event when value close to max) equals bucketLength, default to index of last bucket
        // (e.g. Math.floor(10000 / 416) == 24 && bucketLength == 24)
        if (bucketIndex >= bucketLength) bucketIndex = (int) bucketLength - 1;

        return bucketIndex;
    }
}
