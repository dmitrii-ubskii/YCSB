/*
 * Copyright (c) 2018 - 2019 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.typedb;

import com.typedb.driver.api.answer.ConceptDocumentIterator;
import com.typedb.driver.api.answer.JSON;
import com.typedb.driver.common.exception.TypeDBDriverException;
import site.ycsb.*;
import site.ycsb.Status;

import java.util.*;

/**
 * TypeDB binding
 * <p>
 * See {@code typedb/README.md} for details.
 */
public class FetchTypeDBClient extends TypeDBClient {
  private String fetchFields(final Set<String> fields) {
    StringBuilder query = new StringBuilder(
        "fetch { \"fields\": [ match ($key, $field) isa key-field; $field has id $id, has val $val;");
    if (fields != null) {
      boolean first = true;
      for (String field : fields) {
        if (first) {
          first = false;
        } else {
          query.append(" or ");
        }
        query.append("{ $id == \"").append(escape(field)).append("\"; }");
      }
      query.append("; ");
    }
    query.append("fetch { \"id\": $id, \"value\": $val }; ] };");
    return query.toString();
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(
      final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result
  ) {
    try {
      String ycsbTable = "ycsb-" + table;
      ensureTransaction(ycsbTable);
      String query = "match $key isa key, has id \"" + escape(key) + "\"; limit 1;" + fetchFields(fields);
      ConceptDocumentIterator response = transaction().query(query).resolve().asConceptDocuments();
      if (!response.hasNext()) {
        return Status.NOT_FOUND;
      }
      JSON json = response.next();
      for (JSON entry : json.asObject().get("fields").asArray()) {
        result.put(entry.asObject().get("id").asString(), decode(entry.asObject().get("value").asString()));
      }
      return Status.OK;
    } catch (final TypeDBDriverException e) {
      logger().error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startKey    The record key of the first record to read.
   * @param recordCount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   * class's description for a discussion of error codes.
   */
  @Override
  public Status scan(
      String table, String startKey, int recordCount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result
  ) {
    try {
      String ycsbTable = "ycsb-" + table;
      ensureTransaction(ycsbTable);
      String query = "match $key isa key, has id $kid; $kid >= \"" + escape(startKey) + "\"; " + "sort $kid; limit " +
          recordCount + "; " + fetchFields(fields);
      ConceptDocumentIterator response = transaction().query(query).resolve().asConceptDocuments();
      if (!response.hasNext()) {
        return Status.NOT_FOUND;
      }
      response.stream().forEach(json -> {
          HashMap<String, ByteIterator> resultMap = new HashMap<>();
          for (JSON entry : json.asObject().get("fields").asArray()) {
            resultMap.put(entry.asObject().get("id").asString(), decode(entry.asObject().get("value").asString()));
          }
          result.add(resultMap);
        }
      );
      return Status.OK;
    } catch (final TypeDBDriverException e) {
      logger().error(e.getMessage(), e);
      return Status.ERROR;
    }
  }
}
