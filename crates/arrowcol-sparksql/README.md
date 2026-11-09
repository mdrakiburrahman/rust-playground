# Spark SQL catalog syntax

| Purpose | Contract |
| --- | --- |
| Parser | `parse(&str) -> Result<Statement, Vec<ParseError>>`, UTF-8 byte spans |
| Renderer | `Statement::to_spark_sql()`, deterministic properties and quoting |
| Runtime | Rust + chumsky; no JVM, Spark execution, or DataFusion dialect |
| Verify | `npx nx run arrowcol-sparksql:verify` |

| Statement | Supported form |
| --- | --- |
| Create table | `CREATE TABLE [IF NOT EXISTS] cat.db.tbl (columns) USING DELTA` |
| Table clauses | `PARTITIONED BY (existing_column, ...)`, `LOCATION`, `COMMENT`, `TBLPROPERTIES` in any order; each at most once |
| Add columns | `ALTER TABLE cat.db.tbl ADD COLUMNS (...)` or `ADD COLUMN name type`; additions must be nullable |
| Properties | `ALTER TABLE ... SET TBLPROPERTIES ('key'='value')`, `UNSET TBLPROPERTIES [IF EXISTS] ('key')` |
| Namespace | `CREATE DATABASE/SCHEMA [IF NOT EXISTS] name [COMMENT '...'] [LOCATION '...'] [WITH DBPROPERTIES (...)]` |
| Inspect | `SHOW TABLES [IN namespace] [LIKE 'pattern']`, `SHOW TBLPROPERTIES table [('key')]`, `DESCRIBE [TABLE] table` |
| Drop | `DROP TABLE [IF EXISTS] table`; service authorization remains separate |
| Types | `BOOLEAN`, signed integers, `FLOAT`, `DOUBLE`, `STRING`, `BINARY`, `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `DECIMAL(p,s)`, `STRUCT<name: type>`, `ARRAY<type>`, `MAP<key,value>` |

Spark integer aliases are normalized. `TIMESTAMP` uses microseconds and the catalog
session timezone; `TIMESTAMP_NTZ` has no timezone. Decimal precision is 1–38,
scale is 0–precision. Map keys are boolean, integral, or string for portable
schema contracts. Backtick identifiers preserve spelling; unquoted identifiers
are normalized to lowercase.

Everything outside this subset is rejected, including CTAS/SELECT, defaults,
constraints, generated columns, partition transforms, Hive typed partition
declarations, non-Delta providers, and DataFusion/Arrow physical type extensions.
Input is limited to 65,536 bytes and 64 levels of type/clause nesting.
SQL line comments and non-nested block comments are accepted.
Arrow unsigned integers, non-microsecond timestamps, fixed/view/large binaries,
dictionaries, and nonstandard lists have no lossless Spark DDL representation.
