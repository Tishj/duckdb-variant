#include "variant_extension.hpp"
#include "variant_functions.hpp"

#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

static Value ConvertVariant(RecursiveUnifiedVectorFormat &source, idx_t row, idx_t values_idx) {
	//! values
	auto &values = UnifiedVariantVector::GetValues(source);
	auto values_data = values.GetData<list_entry_t>(values);

	//! type_ids
	auto &type_ids = UnifiedVariantVector::GetValuesTypeId(source);
	auto type_ids_data = type_ids.GetData<uint8_t>(type_ids);

	//! byte_offsets
	auto &byte_offsets = UnifiedVariantVector::GetValuesByteOffset(source);
	auto byte_offsets_data = byte_offsets.GetData<uint32_t>(byte_offsets);

	//! children
	auto &children = UnifiedVariantVector::GetChildren(source);
	auto children_data = children.GetData<list_entry_t>(children);

	//! value_ids
	auto &value_ids = UnifiedVariantVector::GetChildrenValueId(source);
	auto value_ids_data = value_ids.GetData<uint32_t>(value_ids);

	//! key_ids
	auto &key_ids = UnifiedVariantVector::GetChildrenKeyId(source);
	auto key_ids_data = key_ids.GetData<uint32_t>(key_ids);

	//! keys
	auto &keys = UnifiedVariantVector::GetKeys(source);
	auto keys_data = keys.GetData<list_entry_t>(keys);
	auto &keys_entry = UnifiedVariantVector::GetKeysEntry(source);
	auto keys_entry_data = keys_entry.GetData<string_t>(keys_entry);

	//! list entries
	auto keys_list_entry = keys_data[keys.sel->get_index(row)];
	auto children_list_entry = children_data[children.sel->get_index(row)];
	auto values_list_entry = values_data[values.sel->get_index(row)];

	//! The 'values' data of the value we're currently converting
	values_idx += values_list_entry.offset;
	auto type_id = static_cast<VariantLogicalType>(type_ids_data[type_ids.sel->get_index(values_idx)]);
	auto byte_offset = byte_offsets_data[byte_offsets.sel->get_index(values_idx)];

	//! The blob data of the Variant, accessed by byte offset retrieved above ^
	auto &value = UnifiedVariantVector::GetValue(source);
	auto value_data = value.GetData<string_t>(value);
	auto &blob = value_data[row];
	auto blob_data = const_data_ptr_cast(blob.GetData());

	auto ptr = const_data_ptr_cast(blob_data + byte_offset);
	switch (type_id) {
	case VariantLogicalType::VARIANT_NULL:
		return "NULL";
	case VariantLogicalType::BOOL_TRUE:
		return "true";
	case VariantLogicalType::BOOL_FALSE:
		return "false";
	case VariantLogicalType::INT8:
		return Value::TINYINT(Load<int8_t>(ptr));
	case VariantLogicalType::INT16:
		return Value::SMALLINT(Load<int16_t>(ptr));
	case VariantLogicalType::INT32:
		return Value::INTEGER(Load<int32_t>(ptr));
	case VariantLogicalType::INT64:
		return Value::BIGINT(Load<int64_t>(ptr));
	case VariantLogicalType::INT128:
		return Value::HUGEINT(Load<hugeint_t>(ptr));
	case VariantLogicalType::UINT8:
		return Value::UTINYINT(Load<uint8_t>(ptr));
	case VariantLogicalType::UINT16:
		return Value::USMALLINT(Load<uint16_t>(ptr));
	case VariantLogicalType::UINT32:
		return Value::UINTEGER(Load<uint32_t>(ptr));
	case VariantLogicalType::UINT64:
		return Value::UBIGINT(Load<uint64_t>(ptr));
	case VariantLogicalType::UINT128:
		return Value::UHUGEINT(Load<uhugeint_t>(ptr));
	case VariantLogicalType::UUID:
		return Value::UUID(Load<hugeint_t>(ptr));
	case VariantLogicalType::INTERVAL:
		return Value::INTERVAL(Load<interval_t>(ptr));
	case VariantLogicalType::FLOAT:
		return Value::FLOAT(Load<float>(ptr));
	case VariantLogicalType::DOUBLE:
		return Value::DOUBLE(Load<double>(ptr));
	case VariantLogicalType::DATE:
		return Value::DATE(date_t(Load<int32_t>(ptr)));
	case VariantLogicalType::BLOB: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		return Value::BLOB(const_data_ptr_cast(string_data), string_length);
	}
	case VariantLogicalType::VARCHAR: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		return Value(string_t(string_data, string_length));
	}
	case VariantLogicalType::DECIMAL: {
		auto width = VarintDecode<idx_t>(ptr);
		auto scale = VarintDecode<idx_t>(ptr);

		if (width > DecimalWidth<int64_t>::max) {
			return Value::DECIMAL(Load<hugeint_t>(ptr), width, scale);
		} else if (width > DecimalWidth<int32_t>::max) {
			return Value::DECIMAL(Load<int64_t>(ptr), width, scale);
		} else if (width > DecimalWidth<int16_t>::max) {
			return Value::DECIMAL(Load<int32_t>(ptr), width, scale);
		} else {
			return Value::DECIMAL(Load<int16_t>(ptr), width, scale);
		}
	}
	case VariantLogicalType::TIME_MICROS:
		return Value::TIME(Load<dtime_t>(ptr));
	case VariantLogicalType::TIME_MICROS_TZ:
		return Value::TIMETZ(Load<dtime_tz_t>(ptr));
	case VariantLogicalType::TIMESTAMP_MICROS:
		return Value::TIMESTAMP(Load<timestamp_t>(ptr));
	case VariantLogicalType::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(Load<timestamp_sec_t>(ptr));
	case VariantLogicalType::TIMESTAMP_NANOS:
		return Value::TIMESTAMPNS(Load<timestamp_ns_t>(ptr));
	case VariantLogicalType::TIMESTAMP_MILIS:
		return Value::TIMESTAMPMS(Load<timestamp_ms_t>(ptr));
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return Value::TIMESTAMPTZ(Load<timestamp_tz_t>(ptr));
	case VariantLogicalType::ARRAY: {
		auto count = VarintDecode<uint32_t>(ptr);
		vector<string> children;
		if (count) {
			auto child_index_start = VarintDecode<uint32_t>(ptr);
			for (idx_t i = 0; i < count; i++) {
				auto index = value_ids.sel->get_index(children_list_entry.offset + child_index_start + i);
				auto child_index = value_ids_data[index];
				auto val = ConvertVariant(source, row, child_index);
				children.push_back(val.ToString());
			}
		}
		return StringUtil::Format("[%s]", StringUtil::Join(children, ", "));
	}
	case VariantLogicalType::OBJECT: {
		auto count = VarintDecode<uint32_t>(ptr);
		vector<string> children;
		if (count) {
			auto child_index_start = VarintDecode<uint32_t>(ptr);
			for (idx_t i = 0; i < count; i++) {
				auto children_index = value_ids.sel->get_index(children_list_entry.offset + child_index_start + i);
				auto child_value_idx = value_ids_data[children_index];
				auto val = ConvertVariant(source, row, child_value_idx);

				auto key_ids_index = key_ids.sel->get_index(children_list_entry.offset + child_index_start + i);
				auto child_key_id = key_ids_data[key_ids_index];
				auto &key = keys_entry_data[keys_entry.sel->get_index(keys_list_entry.offset + child_key_id)];
				children.push_back(StringUtil::Format("'%s': %s", key.GetString(), val.ToString()));
			}
		}
		return StringUtil::Format("{%s}", StringUtil::Join(children, ", "));
	}
	default:
		throw InternalException("VariantLogicalType(%d) not handled", static_cast<uint8_t>(type_id));
	}

	return nullptr;
}

bool VariantFunctions::CastVARIANTToVARCHAR(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(source, count, source_format);

	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {

		auto val = ConvertVariant(source_format, i, 0);
		result_data[i] = StringVector::AddString(result, val.ToString());
	}

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return true;
}

} // namespace duckdb
