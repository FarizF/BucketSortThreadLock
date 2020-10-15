import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class Driver {

    private static int processors = 1;

    public static final Path USER_DIR_PATH = Paths.get(System.getProperty("user.dir"));
    public static final FileSystem FILE_SYSTEM = USER_DIR_PATH.getFileSystem();
    public static final File OUTPUT_FILE_SEQUENTIAL = new File(System.getProperty("user.dir") + FILE_SYSTEM.getSeparator() + "bucketSortAWFFoutputSeq.txt");
    public static final File OUTPUT_FILE_PARALLEL = new File(System.getProperty("user.dir") + FILE_SYSTEM.getSeparator() + "bucketSortAWFFoutputPar.txt");
    private static HashMap<String, TreeMap<String, Long[]>> resultAggregatesMap = new HashMap<>();

    public static void main(String[] args) throws IOException {

        int runCount;

        try {
            runCount = Integer.parseInt(args[0]);
        } catch (NumberFormatException|ArrayIndexOutOfBoundsException e) {
            runCount = 1;
        }

        // Clear files
        Files.writeString(OUTPUT_FILE_SEQUENTIAL.toPath(), "");
        Files.writeString(OUTPUT_FILE_PARALLEL.toPath(), "");

        System.out.println("Output file location: " + OUTPUT_FILE_SEQUENTIAL.toPath().toString());
        for (int i = 0; i < runCount; i++) {
            System.out.println("Starting global run: " + (i + 1) + "/" + runCount + ".");
            for (int k = 0; k < 4; k++) {
                System.out.println("Using " + processors + " threads to sort(parallel only).");
                System.out.println("Using " + processors + " buckets to sort.");
                int max = 50000;

                for (int j = 0; j < 4; j++) {
                    long startTime = System.currentTimeMillis();
                    long duration;
                    System.out.print("Generating input(" + max + "): ");
                    int[] formattedInput = generateInput(max);
                    System.out.println("took " + ((System.currentTimeMillis() - startTime)) + " milliseconds.");

                    int[] inputClone = formattedInput.clone();

                    String currentRunString = (k+1) + " - " + (j+1);
                    System.out.println("Run #: " + currentRunString + "\n");

                    BucketSort bucketSort = new BucketSort();
                    System.out.println("Sequential bucketsort: ");
                    startTime = System.currentTimeMillis();
                    String sortedResultSequential = Arrays.toString(bucketSort.sequentialBucketSort(inputClone, max, processors));
                    duration = System.currentTimeMillis() - startTime;
                    System.out.println("Sequential bucketsort took " + duration + " milliseconds.");

                    addToResultsMap("Sequential bucketsort", currentRunString, runCount, duration, i);

                    Files.writeString(OUTPUT_FILE_SEQUENTIAL.toPath(),
                            "Run #: " + (k + 1) + " - " + (j + 1) + "\n" + sortedResultSequential + "\n", StandardOpenOption.APPEND
                    );

                    inputClone = formattedInput.clone();

                    currentRunString = (k+1) + " - " + (j+1);
                    startTime = System.currentTimeMillis();
                    System.out.println("Parallel bucketsort: ");
                    Files.writeString(OUTPUT_FILE_PARALLEL.toPath(), "Run #: " + (k + 1) + " - " + (j + 1) + "\n", StandardOpenOption.APPEND);
                    bucketSort.parallelBucketSort(inputClone, max, processors);
                    duration = System.currentTimeMillis() - startTime;
                    System.out.println("Parallel bucketsort took " + duration + " milliseconds.\n");
                    addToResultsMap("Parallel bucketsort", currentRunString, runCount, duration, i);

                    max = max * 2;
                }

                if (processors * 2 > Runtime.getRuntime().availableProcessors()) {
                    System.out.println("Cannot use " + processors * 2 + " threads or more.");
                    break;
                } else {
                    processors = processors * 2;
                }
            }
            processors = 1;
        }

        printResultAverages();

        System.out.println("If assessment was finished, please delete the output files situated in the root folder of this project.");
    }

    /**
     * @return Generated collection of randomized primitive integers
     */
    private static int[] generateInput(int max) {
        Random ran = new Random();
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < max; i++) {
            int min = 0;
            stringBuilder.append(min + ran.nextInt(max - min + 1));

            if (i != max - 1) {
                stringBuilder.append(",");
            }
        }

        String longString = stringBuilder.toString();

        String[] splitInput = longString.split(",");
        int[] formattedInput = new int[splitInput.length];

        for (int i = 0; i < splitInput.length; i++)
            formattedInput[i] = Integer.parseInt(splitInput[i]);

        return formattedInput;
    }

    private static void addToResultsMap(String algorithm, String currentRunString, int runCount, long duration, int i) {
        if (resultAggregatesMap.get(algorithm) != null) {
            TreeMap<String, Long[]> currentInnerMap = resultAggregatesMap.get(algorithm);
            if (currentInnerMap.get(currentRunString) != null) {
                Long[] newLongArray = currentInnerMap.get(currentRunString);
                newLongArray[i] = duration;

            } else {
                Long[] newLongArray = new Long[runCount];
                newLongArray[0] = duration;
                currentInnerMap.put(currentRunString, newLongArray);
            }
        } else {
            TreeMap<String, Long[]> runTreeMap = new TreeMap<>();
            Long[] newLongArray = new Long[runCount];
            newLongArray[0] = duration;
            runTreeMap.put(currentRunString, newLongArray);
            resultAggregatesMap.put(algorithm, runTreeMap);
        }
    }

    private static void printResultAverages() {
        for (Map.Entry<String, TreeMap<String, Long[]>> algorithmMap : resultAggregatesMap.entrySet()) {
            System.out.println(algorithmMap.getKey() + " result averages:");

            for (Map.Entry<String, Long[]> runResultsMapEntrySet : algorithmMap.getValue().entrySet()) {
                System.out.print(runResultsMapEntrySet.getKey());
                double runAverage = 0;
                for (Long runResult : runResultsMapEntrySet.getValue())
                    runAverage += runResult;

                System.out.println(": " + (int) runAverage / runResultsMapEntrySet.getValue().length + "ms");
            }

            System.out.println();
        }
    }
}
