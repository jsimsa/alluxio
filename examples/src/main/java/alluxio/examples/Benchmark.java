package alluxio.examples;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;

import com.beust.jcommander.IDefaultProvider;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Benchmark {
  private enum Type {
    METADATA,
    READ,
    WRITE
  }

  @Parameter(
      names = {"--benchmark-type", "-bt"},
      description = "The type of the benchmark to run.",
      required = true
  )
  private Type mType;

  @Parameter(
      names = {"--file-size", "-fs"},
      description = "The file size to use."
  )
  private int mFileSize;

  @Parameter(
      names = {"--block-size", "-bs"},
      description = "The file size to use."
  )
  private int mBlocksSize;

  @Parameter(
      names = {"--num-operations", "-no"},
      description = "Total number of operations to perform.",
      required = true
  )
  private int mNumOperations;

  @Parameter(
      names = {"--num-iterations", "-ni"},
      description = "Total number of iterations to perform."
  )
  private int mNumIterations;

  @Parameter(
      names = {"--num-threads", "-nt"},
      description = "Concurrency level."
  )
  private int mNumThreads;

  @Parameter(
      names = {"--buffer-size", "-us"},
      description = "Size of the buffer to use when performing IO."
  )
  private int mBufferSize;

  private static final IDefaultProvider DEFAULT_PROVIDER = new IDefaultProvider() {
    @Override
    public String getDefaultValueFor(String option) {
      if ("--benchmark-type".equals(option) || "-bt".equals(option)) {
        return "METADATA";
      }
      if ("--block-size".equals(option) || "-bs".equals(option)) {
        return "134217728"; // 128 MB
      }
      if ("--buffer-size".equals(option) || "-us".equals(option)) {
        return "1048576"; // 1 MB
      }
      if ("--file-size".equals(option) || "-fs".equals(option)) {
        return "268435456"; // 256 MB
      }
      if ("--num-iterations".equals(option) || "-ni".equals(option)) {
        return "10";
      }
      if ("--num-operations".equals(option) || "-no".equals(option)) {
        return "1";
      }
      if ("--num-threads".equals(option) || "-nt".equals(option)) {
        return "1";
      }
      return "";
    }
  };

  private static final int KB = 1024;
  private static final int MB = 1024*KB;
  private static final int GB = 1024*MB;

  /**
   * Entry point for the {@link Benchmark} program.
   *
   * @param args command-line arguments
   */
  public static void main(String []args) {
    Benchmark benchmark = new Benchmark();
    JCommander jc = new JCommander();
    jc.setDefaultProvider(DEFAULT_PROVIDER);
    jc.addObject(benchmark);
    jc.parse(args);
    switch (benchmark.mType) {
      case METADATA:
        metadataBenchmark(benchmark);
        break;
      case READ:
        readBenchmark(benchmark);
        break;
      case WRITE:
        writeBenchmark(benchmark);
        break;
    }
  }

  public static void metadataBenchmark(Benchmark bench) {
    final FileSystem fileSystem = FileSystem.Factory.get();
    final ExecutorService executor = Executors.newFixedThreadPool(bench.mNumThreads);
    final float totalRpcs = bench.mNumOperations * bench.mNumThreads;

    int total = 0;
    try {
      for (int i = 0; i < bench.mNumIterations; i++) {
        if (fileSystem.exists(new AlluxioURI("/data"))) {
          fileSystem.delete(new AlluxioURI("/data"));
        }
        byte[] data = new byte[bench.mFileSize];
        FileOutStream os = fileSystem.createFile(new AlluxioURI("/data"));
        os.write(data);
        os.close();
        List<Callable<Void>> callables = new ArrayList<>();
        for (int j = 0; j < bench.mNumThreads; j++) {
          callables.add(new CheckFileExistence(bench, "/data"));
        }
        long start = System.currentTimeMillis();
        executor.invokeAll(callables);
        long end = System.currentTimeMillis();
        System.out.printf("Iteration #%d: %.2f RPCs in %d milliseconds.\n", i + 1, totalRpcs,
            end - start);
        total += end - start;
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      executor.shutdown();
    }
    int average = total / bench.mNumIterations;
    System.out
        .printf("Average time %.2f secs, average throughput %.2f RPC/s\n", ((float) average) / 1000,
            totalRpcs * 1000 / (float) average);
  }


  public static void readBenchmark(Benchmark bench) {
    final FileSystem fileSystem = FileSystem.Factory.get();
    final ExecutorService executor = Executors.newFixedThreadPool(bench.mNumThreads);
    final float totalBytes =
        (float) bench.mNumOperations * bench.mNumThreads * ((float) bench.mFileSize / (float) GB);

    int total = 0;
    try {
      for (int i = 0; i < bench.mNumIterations; i++) {
        if (fileSystem.exists(new AlluxioURI("/data"))) {
          fileSystem.delete(new AlluxioURI("/data"));
        }
        byte[] data = new byte[bench.mFileSize];
        FileOutStream os = fileSystem.createFile(new AlluxioURI("/data"));
        os.write(data);
        os.close();
        List<Callable<Void>> callables = new ArrayList<>();
        for (int j = 0; j < bench.mNumThreads; j++) {
          callables.add(new ReadFile(bench, "/data"));
        }
        long start = System.currentTimeMillis();
        executor.invokeAll(callables);
        long end = System.currentTimeMillis();
        System.out.printf("Iteration #%d: read %.2fGBs in %d milliseconds.\n", i + 1, totalBytes,
            end - start);
        total += end - start;
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      executor.shutdown();
    }
    int average = total / bench.mNumIterations;
    System.out
        .printf("Average time %.2f secs, average throughput %.2f GB/s\n", ((float) average) / 1000,
            totalBytes * 1000 / (float) average);
  }

  public static void writeBenchmark(Benchmark bench) {
    FileSystem fileSystem = FileSystem.Factory.get();
    ExecutorService executor = Executors.newFixedThreadPool(bench.mNumThreads);
    final float totalBytes =
        (float) bench.mNumOperations * bench.mNumThreads * ((float) bench.mFileSize / (float) GB);

    int total = 0;
    try {
      for (int i = 0; i < bench.mNumIterations; i++) {
        for (int j = 0; j < bench.mNumOperations; j++) {
          for (int k = 0; k < bench.mNumThreads; k++) {
            if (fileSystem.exists(new AlluxioURI(String.format("/data-%d-%d", j + 1, k + 1)))) {
              fileSystem.delete(new AlluxioURI(String.format("/data-%d-%d", j + 1, k + 1)));
            }
          }
        }
        byte[] data = new byte[bench.mBufferSize];
        Random random = new Random();
        random.nextBytes(data);
        List<Callable<Void>> callables = new ArrayList<>();
        for (int k = 0; k < bench.mNumThreads; k++) {
          callables.add(new WriteFile(bench,"/data", k + 1, data));
        }
        long start = System.currentTimeMillis();
        executor.invokeAll(callables);
        long end = System.currentTimeMillis();
        System.out.printf("Iteration #%d: wrote %.2fGBs in %d milliseconds.\n", i + 1, totalBytes,
            end - start);
        total += end - start;
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      executor.shutdown();
    }
    int average = total / bench.mNumIterations;
    System.out
        .printf("Average time %.2f secs, average throughput %.2f GB/s\n", ((float) average) / 1000,
            totalBytes * 1000 / (float) average);
  }

  private static class ReadFile implements Callable<Void> {
    private FileSystem mFileSystem;
    private int mNumFiles;
    private String mPath;
    private byte[] mData;
    private int mBytesToWrite;

    private ReadFile(Benchmark benchmark, String path) {
      mFileSystem = FileSystem.Factory.get();
      mNumFiles = benchmark.mNumOperations;
      mPath = path;
      mData = new byte[benchmark.mBufferSize];
      mBytesToWrite = benchmark.mFileSize;
    }

    @Override
    public Void call() throws Exception {
      for (int i = 0; i < mNumFiles; i++) {
        FileInStream is = mFileSystem.openFile(new AlluxioURI(mPath));
        while (mBytesToWrite > 0) {
          try {
            mBytesToWrite -= is.read(mData, 0, Math.min(mBytesToWrite, mData.length));
          } catch (IOException e) {
            System.out.println(e.getMessage());
          }
        }
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
    private int mBytesToWrite;
    private CreateFileOptions mOptions;

    private WriteFile(Benchmark benchmark, String path, int id, byte[] data) {
      mFileSystem = FileSystem.Factory.get();
      mId = id;
      mNumFiles = benchmark.mNumOperations;
      mPath = path;
      mData = data;
      mBytesToWrite = benchmark.mFileSize;
      mOptions = CreateFileOptions.defaults().setBlockSizeBytes(benchmark.mBlocksSize);
    }

    @Override
    public Void call() throws Exception {
      for (int i = 0; i < mNumFiles; i++) {
        FileOutStream os = mFileSystem
            .createFile(new AlluxioURI(String.format("%s-%d-%d", mPath, i + 1, mId)), mOptions);
        while (mBytesToWrite > 0) {
          try {
            os.write(mData, 0, Math.min(mBytesToWrite, mData.length));
            mBytesToWrite -= mData.length;
          } catch (IOException e) {
            System.out.println(e.getMessage());
          }
        }
        os.close();
      }
      return null;
    }
  }

  private static class CheckFileExistence implements Callable<Void> {
    private FileSystem mFileSystem;
    private String mPath;
    private int mNumOperations;

    private CheckFileExistence(Benchmark benchmark, String path) {
      mFileSystem = FileSystem.Factory.get();
      mPath = path;
      mNumOperations = benchmark.mNumOperations;
    }

    @Override
    public Void call() throws Exception {
      for (int i = 0; i < mNumOperations; i++) {
        mFileSystem.exists(new AlluxioURI(mPath));
      }
      return null;
    }
  }
}
