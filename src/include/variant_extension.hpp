#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

enum class VariantLogicalType : uint8_t {
	VARIANT_NULL = 0,
	BOOL_TRUE = 1,
	BOOL_FALSE = 2,
	INT8 = 3,
	INT16 = 4,
	INT32 = 5,
	INT64 = 6,
	INT128 = 7,
	UINT8 = 8,
	UINT16 = 9,
	UINT32 = 10,
	UINT64 = 11,
	UINT128 = 12,
	FLOAT = 13,
	DOUBLE = 14,
	DECIMAL = 15,
	VARCHAR = 16,
	BLOB = 17,
	UUID = 18,
	DATE = 19,
	TIME_MICROS = 20,
	TIME_NANOS = 21,
	TIMESTAMP_SEC = 22,
	TIMESTAMP_MILIS = 23,
	TIMESTAMP_MICROS = 24,
	TIMESTAMP_NANOS = 25,
	TIME_MICROS_TZ = 26,
	TIMESTAMP_MICROS_TZ = 27,
	INTERVAL = 28,
	OBJECT = 29,
	ARRAY = 30
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
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
};

} // namespace duckdb
