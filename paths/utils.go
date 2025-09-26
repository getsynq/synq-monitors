package paths

import (
	"strings"

	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
)

var ValidMonitoredTypes []entitiesv1.EntityType = []entitiesv1.EntityType{
	// Tables
	entitiesv1.EntityType_ENTITY_TYPE_CLICKHOUSE_TABLE,
	entitiesv1.EntityType_ENTITY_TYPE_SNOWFLAKE_TABLE,
	entitiesv1.EntityType_ENTITY_TYPE_SNOWFLAKE_DYNAMIC_TABLE,
	entitiesv1.EntityType_ENTITY_TYPE_BQ_TABLE,
	entitiesv1.EntityType_ENTITY_TYPE_REDSHIFT_TABLE,
	entitiesv1.EntityType_ENTITY_TYPE_POSTGRES_TABLE,
	entitiesv1.EntityType_ENTITY_TYPE_MYSQL_TABLE,
	entitiesv1.EntityType_ENTITY_TYPE_DATABRICKS_TABLE,
	entitiesv1.EntityType_ENTITY_TYPE_DUCKDB_TABLE,
	entitiesv1.EntityType_ENTITY_TYPE_TRINO_TABLE,
	// Views
	entitiesv1.EntityType_ENTITY_TYPE_CLICKHOUSE_VIEW,
	entitiesv1.EntityType_ENTITY_TYPE_SNOWFLAKE_VIEW,
	entitiesv1.EntityType_ENTITY_TYPE_SNOWFLAKE_STREAM,
	entitiesv1.EntityType_ENTITY_TYPE_BQ_VIEW,
	entitiesv1.EntityType_ENTITY_TYPE_REDSHIFT_VIEW,
	entitiesv1.EntityType_ENTITY_TYPE_POSTGRES_VIEW,
	entitiesv1.EntityType_ENTITY_TYPE_MYSQL_VIEW,
	entitiesv1.EntityType_ENTITY_TYPE_DATABRICKS_VIEW,
	entitiesv1.EntityType_ENTITY_TYPE_TRINO_VIEW,
	// SqlMesh
	entitiesv1.EntityType_ENTITY_TYPE_SQLMESH_SQL_MODEL,
	entitiesv1.EntityType_ENTITY_TYPE_SQLMESH_PYTHON_MODEL,
	entitiesv1.EntityType_ENTITY_TYPE_SQLMESH_EXTERNAL,
	entitiesv1.EntityType_ENTITY_TYPE_SQLMESH_SEED,
}

func PathWithColons(path string) string {
	return strings.ReplaceAll(path, ".", "::")
}

func PathWithDots(path string) string {
	return strings.ReplaceAll(path, "::", ".")
}
