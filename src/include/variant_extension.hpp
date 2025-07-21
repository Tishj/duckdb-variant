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
	TIME_TZ,
	TIMESTAMP_MICROS_TZ,
	INTERVAL,
	OBJECT,
	ARRAY
};

struct VariantVector {
	//! The 'keys' list (dictionary)
	static Vector &GetKeys(Vector &vec) {
		return *StructVector::GetEntries(vec)[0];
	}
	//! The 'children' list
	static Vector &GetChildren(Vector &vec) {
		return *StructVector::GetEntries(vec)[1];
	}
	//! The 'key_id' inside the 'children' list
	static Vector &GetChildrenKeyId(Vector &vec) {
		auto &children = ListVector::GetEntry(GetChildren(vec));
		return *StructVector::GetEntries(children)[0];
	}
	//! The 'value_id' inside the 'children' list
	static Vector &GetChildrenValueId(Vector &vec) {
		auto &children = ListVector::GetEntry(GetChildren(vec));
		return *StructVector::GetEntries(children)[1];
	}
	//! The 'values' list
	static Vector &GetValues(Vector &vec) {
		return *StructVector::GetEntries(vec)[2];
	}
	//! The 'type_id' inside the 'values' list
	static Vector &GetValuesTypeId(Vector &vec) {
		auto &values = ListVector::GetEntry(GetValues(vec));
		return *StructVector::GetEntries(values)[0];
	}
	//! The 'byte_offset' inside the 'values' list
	static Vector &GetValuesByteOffset(Vector &vec) {
		auto &values = ListVector::GetEntry(GetValues(vec));
		return *StructVector::GetEntries(values)[1];
	}
	//! The binary blob 'value' encoding the Variant for the row
	static Vector &GetValue(Vector &vec) {
		return *StructVector::GetEntries(vec)[3];
	}
};

class VariantExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
