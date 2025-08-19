/*
 * Copyright (c) 2025 YCSB contributors. All rights reserved.
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

import com.typedb.driver.api.Transaction;
import com.typedb.driver.api.answer.ConceptRowIterator;
import com.typedb.driver.common.exception.TypeDBDriverException;
import site.ycsb.*;
import site.ycsb.Status;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * TypeDB binding
 * <p>
 * See {@code typedb/README.md} for details.
 */
public class KVTypeDBClient extends TypeDBClient {
  private void ensureTable(String table) {
    if (tables().contains(table)) {
      return;
    }
    assert transaction() == null;
    if (!driver().databases().contains(table)) {
      driver().databases().create(table);
    }
    try (Transaction transaction = driver().transaction(table, Transaction.Type.SCHEMA)) {
      String schema = "define\n" + "entity key, owns id @key, owns fields;\n" + "attribute id, value string;\n" +
          "attribute fields, value string;\n";
      transaction.query(schema).resolve();
      transaction.commit();
    }
    tables().add(table);
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
      String query = "match $key isa key, has id \"" + escape(key) + "\", has fields $fields; limit 1;";
      ConceptRowIterator response = transaction().query(query).resolve().asConceptRows();
      if (!response.hasNext()) {
        return Status.NOT_FOUND;
      }
      deserializeValues(decode(response.next().get("fields").get().asAttribute().getString()).toArray(), fields,
          result
      );
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
      String query = "match $key isa key, has id $kid, has fields $fields; $kid >= \"" + escape(startKey) + "\";";
      ConceptRowIterator response = transaction().query(query).resolve().asConceptRows();
      if (!response.hasNext()) {
        return Status.NOT_FOUND;
      }
      response.stream().forEach(row -> {
          final HashMap<String, ByteIterator> values = new HashMap<>();
          deserializeValues(decode(row.get("fields").get().asAttribute().getString()).toArray(), fields, values);
          result.add(values);
        }
      );
      return Status.OK;
    } catch (final TypeDBDriverException e) {
      logger().error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  Status upsert(String table, String key, Map<String, ByteIterator> values) {
    try {
      String ycsbTable = "ycsb-" + table;
      ensureTable(ycsbTable);
      ensureTransaction(ycsbTable);
      StringBuilder query = new StringBuilder("put $key isa key, has id \"").append(escape(key))
          .append("\"; update $key has fields \"").append(encode(serializeValues(values))).append("\";");
      ConceptRowIterator stream = transaction().query(query.toString()).resolve().asConceptRows();
      if (stream.hasNext()) {
        transactionWritesInc();
        return Status.OK;
      } else {
        return Status.NOT_FOUND;
      }
    } catch (final TypeDBDriverException | IOException e) {
      logger().error(e.getMessage(), e);
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
    try {
      String ycsbTable = "ycsb-" + table;
      ensureTable(ycsbTable);
      ensureTransaction(ycsbTable);
      StringBuilder query = new StringBuilder("match $key isa key, has id \"").append(escape(key))
          .append("\"; delete $key;");
      if (!transaction().query(query.toString()).resolve().asConceptRows().hasNext()) {
        return Status.NOT_FOUND;
      }
      transactionWritesInc();
      return Status.OK;
    } catch (final TypeDBDriverException e) {
      logger().error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  // lifted from RocksDBClient
  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while(offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }
}
