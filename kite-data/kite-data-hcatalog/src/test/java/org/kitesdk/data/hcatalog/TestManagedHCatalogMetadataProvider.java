/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.hcatalog;

import org.kitesdk.data.MetadataProvider;
import org.kitesdk.data.TestMetadataProviders;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;

public class TestManagedHCatalogMetadataProvider extends TestMetadataProviders {

  public TestManagedHCatalogMetadataProvider(boolean distributed) {
    super(distributed);
  }

  @Override
  public MetadataProvider newProvider(Configuration conf) {
    return new HCatalogManagedMetadataProvider(conf);
  }

  @After
  public void cleanHCatalog() {
    // ensures all tables are removed
    HCatalog hcat = new HCatalog(conf);
    for (String tableName : hcat.getAllTables("default")) {
      hcat.dropTable("default", tableName);
    }
  }

}
