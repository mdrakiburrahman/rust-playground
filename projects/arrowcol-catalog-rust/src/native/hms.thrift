// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Wire-compatible subset of Apache Hive 4.1.0 hive_metastore.thrift.
// Field and exception IDs are part of the upstream protocol.
// Regenerate: thrift --gen rs --out projects/arrowcol-catalog-rust/src/native
//            projects/arrowcol-catalog-rust/src/native/hms.thrift
namespace rs hms

struct FieldSchema { 1: string name, 2: string type, 3: string comment }
struct SerDeInfo {
  1: string name,
  2: string serializationLib,
  3: map<string, string> parameters
}
struct Order { 1: string col, 2: i32 order }
struct StorageDescriptor {
  1: list<FieldSchema> cols,
  2: string location,
  3: string inputFormat,
  4: string outputFormat,
  5: bool compressed,
  6: i32 numBuckets,
  7: SerDeInfo serdeInfo,
  8: list<string> bucketCols,
  9: list<Order> sortCols,
  10: map<string, string> parameters,
  12: optional bool storedAsSubDirectories
}
struct Table {
  1: string tableName,
  2: string dbName,
  3: string owner,
  4: i32 createTime,
  5: i32 lastAccessTime,
  6: i32 retention,
  7: StorageDescriptor sd,
  8: list<FieldSchema> partitionKeys,
  9: map<string, string> parameters,
  10: string viewOriginalText,
  11: string viewExpandedText,
  12: string tableType,
  14: optional bool temporary = false,
  15: optional bool rewriteEnabled,
  17: optional string catName,
  25: optional i64 id
}
struct Database {
  1: string name,
  2: string description,
  3: string locationUri,
  4: map<string, string> parameters,
  8: optional string catalogName
}
struct GetTableRequest {
  1: required string dbName,
  2: required string tblName,
  4: optional string catName,
  9: optional string processorIdentifier,
  10: optional string engine = "hive"
}
struct GetTableResult { 1: required Table table }
struct GetDatabaseRequest { 1: optional string name, 2: optional string catalogName }
struct SQLNotNullConstraint {
  1: string catName,
  2: string table_db,
  3: string table_name,
  4: string column_name,
  5: string nn_name,
  6: bool enable_cstr,
  7: bool validate_cstr,
  8: bool rely_cstr
}
struct NotNullConstraintsRequest {
  1: required string catName,
  2: required string db_name,
  3: required string tbl_name,
  5: optional i64 tableId = -1
}
struct NotNullConstraintsResponse { 1: required list<SQLNotNullConstraint> notNullConstraints }

// This adapter never sends any other kind of constraint.
struct UnusedConstraint {}
exception MetaException { 1: string message }
exception AlreadyExistsException { 1: string message }
exception InvalidObjectException { 1: string message }
exception NoSuchObjectException { 1: string message }

service ThriftHiveMetastore {
  Database get_database_req(1: GetDatabaseRequest request)
    throws (1: NoSuchObjectException o1, 2: MetaException o2),
  void create_database(1: Database database)
    throws (1: AlreadyExistsException o1, 2: InvalidObjectException o2, 3: MetaException o3),
  GetTableResult get_table_req(1: GetTableRequest req)
    throws (1: MetaException o1, 2: NoSuchObjectException o2),
  NotNullConstraintsResponse get_not_null_constraints(1: NotNullConstraintsRequest request)
    throws (1: MetaException o1, 2: NoSuchObjectException o2),
  void create_table_with_constraints(
    1: Table tbl,
    2: list<UnusedConstraint> primaryKeys,
    3: list<UnusedConstraint> foreignKeys,
    4: list<UnusedConstraint> uniqueConstraints,
    5: list<SQLNotNullConstraint> notNullConstraints,
    6: list<UnusedConstraint> defaultConstraints,
    7: list<UnusedConstraint> checkConstraints
  ) throws (1: AlreadyExistsException o1, 2: InvalidObjectException o2,
            3: MetaException o3, 4: NoSuchObjectException o4)
}
