#!/usr/bin/env bash
set -Eeuo pipefail

sqlcmd=/opt/mssql-tools18/bin/sqlcmd
host="${SQLSERVER_HOST:-sqlserver}"
port="${SQLSERVER_PORT:-1433}"

read_secret() {
  local path="$1"
  local value

  if [[ ! -r "$path" ]]; then
    echo "Required secret is not readable: $path" >&2
    return 64
  fi

  value="$(<"$path")"
  if [[ -z "$value" || "$value" == *$'\n'* || "$value" == *$'\r'* ]]; then
    echo "Secret must be non-empty and single-line: $path" >&2
    return 64
  fi

  printf '%s' "$value"
}

sql_literal_escape() {
  sed "s/'/''/g"
}

export SQLCMDPASSWORD="$(
  read_secret "${SQLSERVER_SA_PASSWORD_FILE:-/run/secrets/sqlserver_sa_password}"
)"

for attempt in $(seq 1 60); do
  if "$sqlcmd" \
    -S "tcp:${host},${port}" \
    -U sa \
    -C \
    -b \
    -l 3 \
    -Q "SET NOCOUNT ON;
        IF EXISTS (
          SELECT 1
          FROM sys.databases
          WHERE [name] = N'arrowcol'
            AND [state_desc] <> N'ONLINE'
        )
          THROW 50000, 'Existing application databases are not online yet.', 1;
        IF DB_ID(N'arrowcol') IS NOT NULL
          EXEC(N'USE [arrowcol]; SELECT TOP (0) [name] FROM sys.database_principals;');
        SELECT 1;" \
    -o /dev/null; then
    break
  fi

  if [[ "$attempt" -eq 60 ]]; then
    echo "SQL Server did not become ready within 120 seconds" >&2
    exit 1
  fi
  sleep 2
done

export ARROWCOL_APP_PASSWORD_SQL="$(
  read_secret "${ARROWCOL_APP_PASSWORD_FILE:-/run/secrets/arrowcol_app_password}" |
    sql_literal_escape
)"
export ARROWCOL_MIGRATOR_PASSWORD_SQL="$(
  read_secret "${ARROWCOL_MIGRATOR_PASSWORD_FILE:-/run/secrets/arrowcol_migrator_password}" |
    sql_literal_escape
)"

"$sqlcmd" \
  -S "tcp:${host},${port}" \
  -U sa \
  -C \
  -b \
  -r 1 \
  -l 30 \
  -d master \
  -i /opt/arrowcol/sql/bootstrap-databases.sql

unset ARROWCOL_APP_PASSWORD_SQL
unset ARROWCOL_MIGRATOR_PASSWORD_SQL

"$sqlcmd" \
  -S "tcp:${host},${port}" \
  -U sa \
  -C \
  -b \
  -l 30 \
  -d master \
  -Q "SET NOCOUNT ON; SELECT [name], state_desc FROM sys.databases WHERE [name] = N'arrowcol' ORDER BY [name];"
