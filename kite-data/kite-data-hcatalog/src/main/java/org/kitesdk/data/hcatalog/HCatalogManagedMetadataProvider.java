/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.hcatalog;

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.MetadataProviderException;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HCatalogManagedMetadataProvider extends HCatalogMetadataProvider {

  private static final Logger logger = LoggerFactory
      .getLogger(HCatalogManagedMetadataProvider.class);

  public HCatalogManagedMetadataProvider(Configuration conf) {
    super(conf);
  }

  @Override
  public DatasetDescriptor load(String name) {
    Preconditions.checkArgument(name != null, "Name cannot be null");
    
    Table table = null;
    String[]  namespace = name.split(".");
    if(namespace.size()>1){
	table = hcat.getTable( namespace[0], namespace[1]);
    }else{
	table = hcat.getTable(HiveUtils.DEFAULT_DB, name);
    }
    i//final Table table = hcat.getTable(HiveUtils.DEFAULT_DB, name);

    if (!TableType.MANAGED_TABLE.equals(table.getTableType())) {
      throw new MetadataProviderException("Table is not managed");
    }

    return HiveUtils.descriptorForTable(conf, table);
  }

  @Override
  public DatasetDescriptor create(String name, DatasetDescriptor descriptor) {
    Preconditions.checkArgument(name != null, "Name cannot be null");
    Preconditions.checkArgument(descriptor != null,
        "Descriptor cannot be null");
    String db = HiveUtils.DEFAULT_DB;
    String[]  namespace = name.split(".");
    if(namespace.size()>1){
	db = namespace[0];
        name = namespace[1];
    }
    if (exists(db,name)) {
      throw new DatasetExistsException(
          "Metadata already exists for dataset:"+db+"."+ name);
    }

    logger.info("Creating a managed Hive table named: "+db+"."+ name);

    // construct the table metadata from a descriptor
    final Table table = HiveUtils.tableForDescriptor(
        name, db,descriptor, false /* managed table */);

    // create it
    hcat.createTable(table);

    // load the created table to get the data location
    final Table newTable = hcat.getTable(db, name);

    return new DatasetDescriptor.Builder(descriptor)
        .location(newTable.getDataLocation())
        .build();
  }
}
