package alluxio.examples;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;

import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Benchmark {

  /**
   * Entry point for the {@link Benchmark} program.
   *
   * @param args command-line arguments
   */
  public static void main(String []args) {
    if (args.length != 0) {
      System.err.println("Usage: ./bin/alluxio runClass alluxio.examples.Benchmark");
      System.exit(-1);
    }
    writeBenchmark(5, 10, 4);
  }

  public static void metadataBenchmark() {
    FileSystem fileSystem = FileSystem.Factory.get();
    final long numOps = 1000000;

    try {
      long start = System.currentTimeMillis();
      for (int i = 0; i < numOps; i++) {
        fileSystem.exists(new AlluxioURI("/data"));
      }
      long end = System.currentTimeMillis();
      System.out.printf("Performed %d operations in %d milliseconds.\n", numOps, end - start);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }


  public static void readBenchmark(int numIterations, int numFiles, int numThreads) {
    final FileSystem fileSystem = FileSystem.Factory.get();
    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    int total = 0;
    try {
      for (int i = 0; i < numIterations; i++) {
        if (fileSystem.exists(new AlluxioURI("/data"))) {
          fileSystem.delete(new AlluxioURI("/data"));
        }
        byte[] data = new byte[1024 * 1024 * 1024];
        FileOutStream os = fileSystem.createFile(new AlluxioURI("/data"));
        os.write(data);
        os.close();
        List<Callable<Void>> callables = new ArrayList<>();
        for (int j = 0; j < numThreads; j++) {
          callables.add(new ReadFile("/data", numFiles));
        }
        long start = System.currentTimeMillis();
        executor.invokeAll(callables);
        long end = System.currentTimeMillis();
        System.out.printf("Iteration #%d: read %dGBs in %d milliseconds.\n", i+1, numFiles * numThreads, end - start);
        total += end - start;
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      executor.shutdown();
    }
    int average = total/numIterations;
    System.out.printf("Average time %.2f secs, average throughput %.2f GB/s", ((float) average) / 1000,
        ((float) numFiles * numThreads * 1000) / (float) average);
  }

  public static void writeBenchmark(int numIterations, int numFiles, int numThreads) {
    FileSystem fileSystem = FileSystem.Factory.get();
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    int total = 0;
    try {
      for (int i = 0; i < numIterations; i++) {
        for (int j = 0; j < numFiles; j++) {
          for (int k = 0; k < numThreads; k++) {
            if (fileSystem.exists(new AlluxioURI(String.format("/data-%d-%d", j + 1, k + 1)))) {
              fileSystem.delete(new AlluxioURI(String.format("/data-%d-%d", j + 1, k + 1)));
            }
          }
        }
        byte[] data = new byte[1024 * 1024 * 1024];
        Random random = new Random();
        random.nextBytes(data);
        List<Callable<Void>> callables = new ArrayList<>();
        for (int k = 0; k < numThreads; k++) {
          callables.add(new WriteFile("/data", k + 1, numFiles, data));
        }
        long start = System.currentTimeMillis();
        executor.invokeAll(callables);
        long end = System.currentTimeMillis();
        System.out.printf("Iteration #%d: wrote %dGBs in %d milliseconds.\n", i+1, numFiles * numThreads, end - start);
        total += end - start;
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      executor.shutdown();
    }
    int average = total/numIterations;
    System.out.printf("Average time %.2f secs, average throughput %.2f GB/s", ((float) average) / 1000,
        ((float) numFiles * numThreads * 1000) / (float) average);
  }

  private static class ReadFile implements Callable<Void> {
    private FileSystem mFileSystem;
    private int mNumFiles;
    private String mPath;

    private ReadFile(String path, int numFiles) {
      mFileSystem = FileSystem.Factory.get();
      mNumFiles = numFiles;
      mPath = path;
    }

    @Override
    public Void call() throws Exception {
      for (int i = 0; i < mNumFiles; i++) {
        FileInStream is = mFileSystem.openFile(new AlluxioURI(mPath));
        IOUtils.copy(is, NullOutputStream.NULL_OUTPUT_STREAM);
        is.close();
      }
      return null;
    }
  }

  private static class WriteFile implements Callable<Void> {
    private FileSystem mFileSystem;
    private int mId;
    private int mNumFiles;
    private String mPath;
    private byte[] mData;

    private WriteFile(String path, int id, int numFiles, byte[] data) {
      mFileSystem = FileSystem.Factory.get();
      mId = id;
      mNumFiles = numFiles;
      mPath = path;
      mData = data;
    }

    @Override
    public Void call() throws Exception {
      for (int i = 0; i < mNumFiles; i++) {
        FileOutStream os =
            mFileSystem.createFile(new AlluxioURI(String.format("%s-%d-%d", mPath, i + 1, mId)));
        os.write(mData);
        os.close();
      }
      return null;
    }
  }
}
