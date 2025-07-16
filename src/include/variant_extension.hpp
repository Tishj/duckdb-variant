#pragma once

#include "duckdb.hpp"

namespace duckdb {

enum class VariantLogicalType : uint8_t {
	VARIANT_NULL,
	BOOL_TRUE,
	BOOL_FALSE,
	INT8,
	INT16,
	INT32,
	INT64,
	INT128,
	UINT8,
	UINT16,
	UINT32,
	UINT64,
	UINT128,
	FLOAT,
	DOUBLE,
	DECIMAL,
	VARCHAR,
	BLOB,
	UUID,
	DATE,
	TIME,
	TIMESTAMP_SEC,
	TIMESTAMP_MILIS,
	TIMESTAMP_MICROS,
	TIMESTAMP_NANOS,
	INTERVAL,
	OBJECT,
	ARRAY
};

class VariantExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
