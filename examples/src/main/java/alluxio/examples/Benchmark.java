package alluxio.examples;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;

import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

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
    writeBenchmark();
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


  public static void readBenchmark() {
    FileSystem fileSystem = FileSystem.Factory.get();
    final long numFiles = 100;

    try {
      if (fileSystem.exists(new AlluxioURI("/data"))) {
        fileSystem.delete(new AlluxioURI("/data"));
      }
      byte[] data = new byte[1024 * 1024 * 1024];
      FileOutStream os = fileSystem.createFile(new AlluxioURI("/data"));
      os.write(data);
      os.close();
      long start = System.currentTimeMillis();
      for (int i = 0; i < numFiles; i++) {
        FileInStream is = fileSystem.openFile(new AlluxioURI("/data"));
        IOUtils.copy(is, NullOutputStream.NULL_OUTPUT_STREAM);
        is.close();
      }
      long end = System.currentTimeMillis();
      System.out.printf("Read %dGBs in %d milliseconds.\n", numFiles, end - start);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  public static void writeBenchmark() {
    FileSystem fileSystem = FileSystem.Factory.get();
    final long numFiles = 100;

    try {
      for (int i = 0; i < numFiles; i++) {
        if (fileSystem.exists(new AlluxioURI(String.format("/data-%d", i + 1)))) {
          fileSystem.delete(new AlluxioURI(String.format("/data-%d", i + 1)));
        }
      }
      byte[] data = new byte[1024 * 1024 * 1024];
      long start = System.currentTimeMillis();
      for (int i = 0; i < numFiles; i++) {
        FileOutStream os = fileSystem.createFile(new AlluxioURI(String.format("/data-%d", i + 1)));
        os.write(data);
        os.close();
      }
      long end = System.currentTimeMillis();
      System.out.printf("Wrote %dGBs in %d milliseconds.\n", numFiles, end - start);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }
}
