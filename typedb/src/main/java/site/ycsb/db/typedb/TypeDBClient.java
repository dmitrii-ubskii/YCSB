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

import com.typedb.driver.TypeDB;
import com.typedb.driver.api.Credentials;
import com.typedb.driver.api.Driver;
import com.typedb.driver.api.DriverOptions;
import com.typedb.driver.api.Transaction;
import com.typedb.driver.api.answer.ConceptDocumentIterator;
import com.typedb.driver.api.answer.ConceptRowIterator;
import com.typedb.driver.api.answer.JSON;
import com.typedb.driver.common.exception.TypeDBDriverException;
import site.ycsb.*;
import site.ycsb.Status;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * TypeDB binding
 * <p>
 * See {@code typedb/README.md} for details.
 */
public class TypeDBClient extends DB {
  private static final Logger LOGGER = LoggerFactory.getLogger(TypeDBClient.class);

  private final Set<String> tables = new HashSet<>();

  @GuardedBy("TypeDBClient.class")
  private static Driver driver = null;

  @Override
  public void init() {
    synchronized (TypeDBClient.class) {
      if (driver == null) {
        driver = TypeDB.driver(TypeDB.DEFAULT_ADDRESS, new Credentials("admin", "password"),
            new DriverOptions(false, null)
        );
      }
    }
  }

  private void ensureTable(String table) {
    if (tables.contains(table)) {
      return;
    }
    if (!driver.databases().contains(table)) {
      driver.databases().create(table);
    }
    try (Transaction transaction = driver.transaction(table, Transaction.Type.SCHEMA)) {
      String schema = "define\n" +
          "entity key, owns id @key, plays key-field:key;\n" +
          "entity field, owns id, owns val, plays key-field:field;\n" +
          "relation key-field, relates key, relates field;\n" + "attribute id, value string;\n" +
          "attribute val, value string;\n";
      transaction.query(schema).resolve();
      transaction.commit();
    }
    tables.add(table);
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    super.cleanup();
  }

  private static String escape(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static String encode(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }

  private static ByteIterator decode(String encoded) {
    return new ByteArrayByteIterator(Base64.getDecoder().decode(encoded));
  }

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
    try (Transaction transaction = driver.transaction("ycsb-" + table, Transaction.Type.READ)) {
      String query = "match $key isa key, has id \"" + escape(key) + "\"; limit 1;" + fetchFields(fields);
      ConceptDocumentIterator response = transaction.query(query).resolve().asConceptDocuments();
      if (!response.hasNext()) {
        return Status.NOT_FOUND;
      }
      JSON json = response.next();
      for (JSON entry : json.asObject().get("fields").asArray()) {
        result.put(entry.asObject().get("id").asString(), decode(entry.asObject().get("value").asString()));
      }
      return Status.OK;
    } catch (final TypeDBDriverException e) {
      LOGGER.error(e.getMessage(), e);
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
    String ycsbTable = "ycsb-" + table;
    try (Transaction transaction = driver.transaction(ycsbTable, Transaction.Type.READ)) {
      String query = "match $key isa key, has id $kid; $kid >= \"" + escape(startKey) + "\"; " + "sort $kid; limit " +
          recordCount + "; " + fetchFields(fields);
      ConceptDocumentIterator response = transaction.query(query).resolve().asConceptDocuments();
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
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   * description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return upsert(table, key, values);
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   * class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return upsert(table, key, values);
  }

  private Status upsert(String table, String key, Map<String, ByteIterator> values) {
    String ycsbTable = "ycsb-" + table;
    ensureTable(ycsbTable);
    try (Transaction transaction = driver.transaction(ycsbTable, Transaction.Type.WRITE)) {
      StringBuilder query = new StringBuilder("put $key isa key, has id \"").append(escape(key)).append("\";");
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        query.append("put $field isa field, has id \"").append(escape(entry.getKey()))
            .append("\"; (key: $key, field: $field) isa key-field;").append("update $field has val \"")
            .append(encode(entry.getValue().toArray())).append("\"; select $key;");
      }
      ConceptRowIterator stream = transaction.query(query.toString()).resolve().asConceptRows();
      if (stream.hasNext()) {
        transaction.commit();
        return Status.OK;
      } else {
        return Status.NOT_FOUND;
      }
    } catch (final TypeDBDriverException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   * class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    String ycsbTable = "ycsb-" + table;
    try (Transaction transaction = driver.transaction(ycsbTable, Transaction.Type.WRITE)) {
      StringBuilder query = new StringBuilder("match $key isa key, has id \"").append(escape(key)).append("\";")
          .append("$kf links ($key, $field), isa key-field; $field isa field;").append("delete $kf; $field;");
      transaction.query(query.toString()).resolve();
      query = new StringBuilder("match $key isa key, has id \"").append(escape(key)).append("\";")
          .append("delete $tk; $key;");
      if (!transaction.query(query.toString()).resolve().asConceptRows().hasNext()) {
        return Status.NOT_FOUND;
      }
      transaction.commit();
      return Status.OK;
    } catch (final TypeDBDriverException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }
}
