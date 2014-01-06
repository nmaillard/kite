/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.filesystem;

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.DatasetWriterException;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.UnknownFormatException;
import org.kitesdk.data.spi.PartitionListener;
import org.kitesdk.data.spi.StorageKey;
import com.google.common.base.Joiner;
import javax.annotation.Nullable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import org.kitesdk.data.spi.StorageKey;

abstract class FileSystemWriters {

  @SuppressWarnings("unchecked") // See https://github.com/Parquet/parquet-mr/issues/106
  public static <E> DatasetWriter<E> newFileWriter(
      FileSystem fs, Path path, DatasetDescriptor descriptor,
      @Nullable PartitionListener partitionListener, @Nullable String name,
      @Nullable StorageKey key) {
    // ensure the path exists
    try {
      fs.mkdirs(path);
      if (partitionListener != null) {
        partitionListener.partitionAdded(name, key);
      }
    } catch (IOException ex) {
      throw new DatasetWriterException("Could not create path:" + path, ex);
    }

    final Format format = descriptor.getFormat();
    final Path file = new Path(path, uniqueFilename(descriptor.getFormat()));

    if (Formats.PARQUET.equals(format)) {
      return new ParquetFileSystemDatasetWriter(fs, file, descriptor.getSchema());
    } else if (Formats.AVRO.equals(format)) {
      return new FileSystemDatasetWriter.Builder()
          .fileSystem(fs)
          .path(file)
          .schema(descriptor.getSchema())
          .build();
    } else {
      throw new UnknownFormatException("Unknown format:" + format);
    }
  }

  private static Joiner DASH = Joiner.on('-');

  private static String uniqueFilename(Format format) {
    // FIXME: This file name is not guaranteed to be truly unique.
    return DASH.join(
        System.currentTimeMillis(),
        Thread.currentThread().getId() + "." + format.getExtension());
  }

}
