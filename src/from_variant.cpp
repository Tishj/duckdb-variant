#include "variant_extension.hpp"
#include "variant_functions.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/type_visitor.hpp"

namespace duckdb {

namespace {

struct VariantNestedData {
	//! The amount of children in the nested structure
	uint32_t child_count;
	//! Index of the first child
	uint32_t children_idx;
};

struct FromVariantConversionData {
	//! The input Variant column
	RecursiveUnifiedVectorFormat unified_format;
	//! If unsuccessful - the error of the conversion
	string error;
};

struct EmptyConversionPayload {};

//! string data
struct StringConversionPayload {
public:
	explicit StringConversionPayload(Vector &vec) : vec(vec) {
	}

public:
	//! The string vector that needs to own the non-inlined data
	Vector &vec;
};

//! decimal
struct DecimalConversionPayload {
public:
	DecimalConversionPayload(idx_t width, idx_t scale) : width(width), scale(scale) {
	}

public:
	idx_t width;
	idx_t scale;
};

} // namespace

string VariantLogicalTypeToString(VariantLogicalType type) {
	switch (type) {
	case VariantLogicalType::VARIANT_NULL:
		return "VARIANT_NULL";
	case VariantLogicalType::BOOL_TRUE:
		return "BOOL_TRUE";
	case VariantLogicalType::BOOL_FALSE:
		return "BOOL_FALSE";
	case VariantLogicalType::INT8:
		return "INT8";
	case VariantLogicalType::INT16:
		return "INT16";
	case VariantLogicalType::INT32:
		return "INT32";
	case VariantLogicalType::INT64:
		return "INT64";
	case VariantLogicalType::INT128:
		return "INT128";
	case VariantLogicalType::UINT8:
		return "UINT8";
	case VariantLogicalType::UINT16:
		return "UINT16";
	case VariantLogicalType::UINT32:
		return "UINT32";
	case VariantLogicalType::UINT64:
		return "UINT64";
	case VariantLogicalType::UINT128:
		return "UINT128";
	case VariantLogicalType::FLOAT:
		return "FLOAT";
	case VariantLogicalType::DOUBLE:
		return "DOUBLE";
	case VariantLogicalType::DECIMAL:
		return "DECIMAL";
	case VariantLogicalType::VARCHAR:
		return "VARCHAR";
	case VariantLogicalType::BLOB:
		return "BLOB";
	case VariantLogicalType::UUID:
		return "UUID";
	case VariantLogicalType::DATE:
		return "DATE";
	case VariantLogicalType::TIME_MICROS:
		return "TIME_MICROS";
	case VariantLogicalType::TIME_NANOS:
		return "TIME_NANOS";
	case VariantLogicalType::TIMESTAMP_SEC:
		return "TIMESTAMP_SEC";
	case VariantLogicalType::TIMESTAMP_MILIS:
		return "TIMESTAMP_MILIS";
	case VariantLogicalType::TIMESTAMP_MICROS:
		return "TIMESTAMP_MICROS";
	case VariantLogicalType::TIMESTAMP_NANOS:
		return "TIMESTAMP_NANOS";
	case VariantLogicalType::TIME_MICROS_TZ:
		return "TIME_MICROS_TZ";
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return "TIMESTAMP_MICROS_TZ";
	case VariantLogicalType::INTERVAL:
		return "INTERVAL";
	case VariantLogicalType::OBJECT:
		return "OBJECT";
	case VariantLogicalType::ARRAY:
		return "ARRAY";
	default:
		return "INVALID TYPE";
	};
}

static bool FinalizeErrorMessage(FromVariantConversionData &conversion_data, Vector &result,
                                 CastParameters &parameters) {
	auto conversion_error = StringUtil::Format("%s to '%s'", conversion_data.error, result.GetType().ToString());
	if (parameters.error_message) {
		*parameters.error_message = conversion_error;
		return false;
	}
	throw ConversionException(conversion_error);
}

//! ------- Primitive Conversion Methods -------

//! bool
struct VariantBooleanConversion {
	using type = bool;
	static bool Convert(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value, bool &ret,
	                    const EmptyConversionPayload &payload, string &error) {
		if (type_id != VariantLogicalType::BOOL_FALSE && type_id != VariantLogicalType::BOOL_TRUE) {
			error = StringUtil::Format("Can't convert from VARIANT(%s)", VariantLogicalTypeToString(type_id));
			return false;
		}
		ret = type_id == VariantLogicalType::BOOL_TRUE;
		return true;
	}
};

//! any direct conversion (int8, date_t, dtime_t, timestamp, etc..)
template <class T, VariantLogicalType TYPE_ID>
struct VariantDirectConversion {
	using type = T;
	static bool Convert(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value, T &ret,
	                    const EmptyConversionPayload &payload, string &error) {
		if (type_id != TYPE_ID) {
			error = StringUtil::Format("Can't convert from VARIANT(%s)", VariantLogicalTypeToString(type_id));
			return false;
		}
		ret = Load<T>(value + byte_offset);
		return true;
	}

	static bool Convert(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value, T &ret,
	                    const StringConversionPayload &payload, string &error) {
		if (type_id != TYPE_ID) {
			error = StringUtil::Format("Can't convert from VARIANT(%s)", VariantLogicalTypeToString(type_id));
			return false;
		}
		auto ptr = value + byte_offset;
		auto length = VarintDecode<idx_t>(ptr);
		ret = StringVector::AddStringOrBlob(payload.vec, reinterpret_cast<const char *>(ptr), length);
		return true;
	}
};

//! decimal
template <class T>
struct VariantDecimalConversion {
	using type = T;
	static constexpr VariantLogicalType TYPE_ID = VariantLogicalType::DECIMAL;
	static bool Convert(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value, T &ret,
	                    const DecimalConversionPayload &payload, string &error) {
		if (type_id != TYPE_ID) {
			error = StringUtil::Format("Can't convert from VARIANT(%s)", VariantLogicalTypeToString(type_id));
			return false;
		}
		auto ptr = value + byte_offset;
		auto width = VarintDecode<idx_t>(ptr);
		auto scale = VarintDecode<idx_t>(ptr);

		if (width != payload.width || scale != payload.scale) {
			error = StringUtil::Format("Can't convert from VARIANT(DECIMAL(%d, %d))", width, scale);
			return false;
		}
		ret = Load<T>(ptr);
		return true;
	}
};

template <class OP, class T = typename OP::type, class PAYLOAD_CLASS>
static bool CastVariantToPrimitive(FromVariantConversionData &conversion_data, Vector &result, uint32_t *value_indices,
                                   idx_t offset, idx_t count, optional_idx row, PAYLOAD_CLASS payload) {
	auto &variant = conversion_data.unified_format;

	auto &target_type = result.GetType();

	auto result_data = FlatVector::GetData<T>(result);
	auto &values_format = UnifiedVariantVector::GetValues(variant);

	auto &type_id_format = UnifiedVariantVector::GetValuesTypeId(variant);
	auto &byte_offset_format = UnifiedVariantVector::GetValuesByteOffset(variant);
	auto &value_format = UnifiedVariantVector::GetValue(variant);

	auto type_id_data = type_id_format.GetData<uint8_t>(type_id_format);
	auto byte_offset_data = byte_offset_format.GetData<uint32_t>(byte_offset_format);
	auto value_data = value_format.GetData<string_t>(value_format);

	auto values_data = values_format.GetData<list_entry_t>(values_format);
	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;
		auto &values_list_entry = values_data[values_format.sel->get_index(row_index)];
		auto blob_index = value_format.sel->get_index(row_index);

		auto value_index = values_list_entry.offset + value_indices[i];
		auto type_id_index = type_id_format.sel->get_index(value_index);
		auto byte_offset_index = byte_offset_format.sel->get_index(value_index);

		auto type_id = static_cast<VariantLogicalType>(type_id_data[type_id_index]);
		auto byte_offset = byte_offset_data[byte_offset_index];
		auto value_blob_data = const_data_ptr_cast(value_data[blob_index].GetData());
		if (type_id == VariantLogicalType::OBJECT || type_id == VariantLogicalType::ARRAY) {
			conversion_data.error =
			    StringUtil::Format("Can't convert VARIANT(%s)", VariantLogicalTypeToString(type_id));
			return false;
		}
		if (!OP::Convert(type_id, byte_offset, value_blob_data, result_data[i + offset], payload,
		                 conversion_data.error)) {
			auto value =
			    VariantConversion::ConvertVariantToValue(conversion_data.unified_format, row_index, value_indices[i]);
			result.SetValue(i + offset, value.DefaultCastAs(target_type, true));
		}
	}
	return true;
}

//! TODO: we can probably template this, where T is `VariantNestedData` for nested types, and stuff like `uint32_t` for
//! UINT32
template <VariantLogicalType TYPE_ID>
static bool CollectNestedData(FromVariantConversionData &conversion_data, uint32_t *value_indices, idx_t count,
                              optional_idx row, VariantNestedData *child_data) {
	auto &variant = conversion_data.unified_format;

	auto &values_format = UnifiedVariantVector::GetValues(variant);
	auto values_data = values_format.GetData<list_entry_t>(values_format);

	auto &type_id_format = UnifiedVariantVector::GetValuesTypeId(variant);
	auto type_id_data = type_id_format.GetData<uint8_t>(type_id_format);

	auto &byte_offset_format = UnifiedVariantVector::GetValuesByteOffset(variant);
	auto byte_offset_data = byte_offset_format.GetData<uint32_t>(byte_offset_format);

	auto &value_format = UnifiedVariantVector::GetValue(variant);
	auto value_data = value_format.GetData<string_t>(value_format);

	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;

		//! values
		auto values_index = values_format.sel->get_index(row_index);
		D_ASSERT(values_format.validity.RowIsValid(values_index));
		auto values_list_entry = values_data[values_index];

		//! Get the index into 'values'
		uint32_t value_index = value_indices[i];

		//! type_id + byte_offset
		auto type_id = static_cast<VariantLogicalType>(
		    type_id_data[type_id_format.sel->get_index(values_list_entry.offset + value_index)]);
		auto byte_offset = byte_offset_data[byte_offset_format.sel->get_index(values_list_entry.offset + value_index)];

		if (type_id != TYPE_ID) {
			conversion_data.error =
			    StringUtil::Format("'%s' was expected, found '%s', can't convert VARIANT",
			                       VariantLogicalTypeToString(TYPE_ID), VariantLogicalTypeToString(type_id));
			return false;
		}

		auto blob_index = value_format.sel->get_index(row_index);
		auto blob_data = const_data_ptr_cast(value_data[blob_index].GetData());

		auto ptr = blob_data + byte_offset;
		child_data[i].child_count = VarintDecode<uint32_t>(ptr);
		child_data[i].children_idx = VarintDecode<uint32_t>(ptr);
	}
	return true;
}

static bool FindValuesWithKey(FromVariantConversionData &conversion_data, const string &key, optional_idx row,
                              uint32_t *res, VariantNestedData *nested_data, idx_t count) {
	auto &source = conversion_data.unified_format;

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

	//! entry of the keys list
	auto &keys_entry = UnifiedVariantVector::GetKeysEntry(source);
	auto keys_entry_data = keys_entry.GetData<string_t>(keys_entry);
	string_t dictionary_key(key.c_str(), key.size());

	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;

		auto &keys_list_entry = keys_data[keys.sel->get_index(row_index)];
		auto &children_list_entry = children_data[children.sel->get_index(row_index)];

		auto &nested_data_entry = nested_data[i];
		bool found_key = false;
		for (idx_t child_idx = 0; child_idx < nested_data_entry.child_count; child_idx++) {
			auto children_index = children_list_entry.offset + nested_data_entry.children_idx + child_idx;
			auto key_id = key_ids_data[key_ids.sel->get_index(children_index)];
			auto value_id = value_ids_data[value_ids.sel->get_index(children_index)];
			auto key_index = keys_entry.sel->get_index(keys_list_entry.offset + key_id);
			auto &child_key = keys_entry_data[key_index];

			if (child_key == dictionary_key) {
				//! Found the key we're looking for
				res[i] = value_id;
				found_key = true;
				break;
			}
		}
		if (!found_key) {
			return false;
		}
	}
	return true;
}

static bool FindValues(FromVariantConversionData &conversion_data, idx_t row_index, uint32_t *res,
                       VariantNestedData &nested_data_entry) {
	auto &source = conversion_data.unified_format;

	//! children
	auto &children = UnifiedVariantVector::GetChildren(source);
	auto children_data = children.GetData<list_entry_t>(children);

	//! value_ids
	auto &value_ids = UnifiedVariantVector::GetChildrenValueId(source);
	auto value_ids_data = value_ids.GetData<uint32_t>(value_ids);

	auto &children_list_entry = children_data[children.sel->get_index(row_index)];
	for (idx_t child_idx = 0; child_idx < nested_data_entry.child_count; child_idx++) {
		auto children_index = children_list_entry.offset + nested_data_entry.children_idx + child_idx;
		auto value_id = value_ids_data[value_ids.sel->get_index(children_index)];
		res[child_idx] = value_id;
	}
	return true;
}

static bool CastVariant(FromVariantConversionData &conversion_data, Vector &result, uint32_t *value_indices,
                        idx_t offset, idx_t count, optional_idx row);

static bool ConvertVariantToList(FromVariantConversionData &conversion_data, Vector &result, uint32_t *value_indices,
                                 idx_t offset, idx_t count, optional_idx row) {
	auto &allocator = Allocator::DefaultAllocator();

	AllocatedData owned_child_data;
	VariantNestedData *child_data = nullptr;
	if (count) {
		owned_child_data = allocator.Allocate(sizeof(VariantNestedData) * count);
		child_data = reinterpret_cast<VariantNestedData *>(owned_child_data.get());
	}

	if (!CollectNestedData<VariantLogicalType::ARRAY>(conversion_data, value_indices, count, row, child_data)) {
		return false;
	}
	idx_t total_children = 0;
	idx_t max_children = 0;
	for (idx_t i = 0; i < count; i++) {
		auto &child_data_entry = child_data[i];
		if (child_data_entry.child_count > max_children) {
			max_children = child_data_entry.child_count;
		}
		total_children += child_data_entry.child_count;
	}

	AllocatedData owned_value_indices;
	uint32_t *new_value_indices = nullptr;
	if (max_children) {
		owned_value_indices = allocator.Allocate(sizeof(uint32_t) * max_children);
		new_value_indices = reinterpret_cast<uint32_t *>(owned_value_indices.get());
	}

	ListVector::Reserve(result, total_children);
	auto &child = ListVector::GetEntry(result);
	auto list_data = ListVector::GetData(result);
	idx_t total_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;
		auto &child_data_entry = child_data[i];

		auto &entry = list_data[i + offset];
		entry.offset = total_offset;
		entry.length = child_data_entry.child_count;
		total_offset += entry.length;

		FindValues(conversion_data, row_index, new_value_indices, child_data_entry);
		CastVariant(conversion_data, child, new_value_indices, entry.offset, child_data_entry.child_count, row_index);
	}
	ListVector::SetListSize(result, total_children);
	return true;
}

static bool ConvertVariantToStruct(FromVariantConversionData &conversion_data, Vector &result, uint32_t *value_indices,
                                   idx_t offset, idx_t count, optional_idx row) {
	auto &target_type = result.GetType();
	auto &allocator = Allocator::DefaultAllocator();

	AllocatedData owned_child_data;
	VariantNestedData *child_data = nullptr;
	if (count) {
		owned_child_data = allocator.Allocate(sizeof(VariantNestedData) * count);
		child_data = reinterpret_cast<VariantNestedData *>(owned_child_data.get());
	}

	//! First get all the Object data from the VARIANT
	if (!CollectNestedData<VariantLogicalType::OBJECT>(conversion_data, value_indices, count, row, child_data)) {
		return false;
	}

	auto &children = StructVector::GetEntries(result);
	auto &child_types = StructType::GetChildTypes(target_type);

	AllocatedData owned_value_indices;
	uint32_t *new_value_indices = nullptr;
	if (count) {
		owned_value_indices = allocator.Allocate(sizeof(uint32_t) * count);
		new_value_indices = reinterpret_cast<uint32_t *>(owned_value_indices.get());
	}

	for (idx_t child_idx = 0; child_idx < child_types.size(); child_idx++) {
		auto &child_name = child_types[child_idx].first;

		//! Then find the relevant child of the OBJECTs we're converting
		//! FIXME: there is nothing preventing an OBJECT from containing the same key twice I believe ?
		if (!FindValuesWithKey(conversion_data, child_name, row, new_value_indices, child_data, count)) {
			conversion_data.error = StringUtil::Format("VARIANT(OBJECT) is missing key '%s'");
			return false;
		}
		//! Now cast all the values we found to the target type
		auto &child = *children[child_idx];
		if (!CastVariant(conversion_data, child, new_value_indices, offset, count, row)) {
			return false;
		}
	}
	return true;
}

//! * @param conversion_data The constant data relevant at all rows of the conversion
//! * @param result The typed Vector to populate in this call
//! * @param value_indices The array of `count` size, containing the (relative, without row offset applied) indices into
//! `values` to convert
//! * @param row The row of the Variant to pull data from, if 'IsValid()' is true
static bool CastVariant(FromVariantConversionData &conversion_data, Vector &result, uint32_t *value_indices,
                        idx_t offset, idx_t count, optional_idx row) {
	auto &target_type = result.GetType();
	auto &error = conversion_data.error;

	if (target_type.IsNested()) {
		switch (target_type.id()) {
		case LogicalTypeId::STRUCT: {
			if (ConvertVariantToStruct(conversion_data, result, value_indices, offset, count, row)) {
				return true;
			}

			for (idx_t i = 0; i < count; i++) {
				auto row_index = row.IsValid() ? row.GetIndex() : i;

				//! Get the index into 'values'
				uint32_t value_index = value_indices[i];
				auto value =
				    VariantConversion::ConvertVariantToValue(conversion_data.unified_format, row_index, value_index);
				result.SetValue(i + offset, value.DefaultCastAs(target_type, true));
			}
			return true;
		}
		case LogicalTypeId::ARRAY:
		case LogicalTypeId::LIST:
		case LogicalTypeId::MAP: {
			if (ConvertVariantToList(conversion_data, result, value_indices, offset, count, row)) {
				return true;
			}
			for (idx_t i = 0; i < count; i++) {
				auto row_index = row.IsValid() ? row.GetIndex() : i;

				//! Get the index into 'values'
				uint32_t value_index = value_indices[i];
				auto value =
				    VariantConversion::ConvertVariantToValue(conversion_data.unified_format, row_index, value_index);
				result.SetValue(i + offset, value.DefaultCastAs(target_type, true));
			}
			return true;
		}
		case LogicalTypeId::UNION: {
			error = "Can't convert VARIANT";
			return false;
		}
		default: {
			error = StringUtil::Format("Nested type: '%s' not handled, can't convert VARIANT", target_type.ToString());
			return false;
		}
		};
	} else {
		EmptyConversionPayload empty_payload;
		switch (target_type.id()) {
		case LogicalTypeId::BOOLEAN:
			return CastVariantToPrimitive<VariantBooleanConversion>(conversion_data, result, value_indices, offset,
			                                                        count, row, empty_payload);
		case LogicalTypeId::TINYINT:
			return CastVariantToPrimitive<VariantDirectConversion<int8_t, VariantLogicalType::INT8>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::SMALLINT:
			return CastVariantToPrimitive<VariantDirectConversion<int16_t, VariantLogicalType::INT16>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::INTEGER:
			return CastVariantToPrimitive<VariantDirectConversion<int32_t, VariantLogicalType::INT32>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::BIGINT:
			return CastVariantToPrimitive<VariantDirectConversion<int64_t, VariantLogicalType::INT64>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::HUGEINT:
			return CastVariantToPrimitive<VariantDirectConversion<hugeint_t, VariantLogicalType::INT128>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::UTINYINT:
			return CastVariantToPrimitive<VariantDirectConversion<uint8_t, VariantLogicalType::UINT8>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::USMALLINT:
			return CastVariantToPrimitive<VariantDirectConversion<uint16_t, VariantLogicalType::UINT16>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::UINTEGER:
			return CastVariantToPrimitive<VariantDirectConversion<uint32_t, VariantLogicalType::UINT32>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::UBIGINT:
			return CastVariantToPrimitive<VariantDirectConversion<uint64_t, VariantLogicalType::UINT64>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::UHUGEINT:
			return CastVariantToPrimitive<VariantDirectConversion<uhugeint_t, VariantLogicalType::UINT128>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::FLOAT:
			return CastVariantToPrimitive<VariantDirectConversion<float, VariantLogicalType::FLOAT>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::DOUBLE:
			return CastVariantToPrimitive<VariantDirectConversion<double, VariantLogicalType::DOUBLE>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::DATE:
			return CastVariantToPrimitive<VariantDirectConversion<date_t, VariantLogicalType::DATE>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::TIMESTAMP:
			return CastVariantToPrimitive<VariantDirectConversion<timestamp_t, VariantLogicalType::TIMESTAMP_MICROS>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::BLOB: {
			StringConversionPayload string_payload(result);
			return CastVariantToPrimitive<VariantDirectConversion<string_t, VariantLogicalType::BLOB>>(
			    conversion_data, result, value_indices, offset, count, row, string_payload);
		}
		case LogicalTypeId::VARCHAR: {
			StringConversionPayload string_payload(result);
			return CastVariantToPrimitive<VariantDirectConversion<string_t, VariantLogicalType::VARCHAR>>(
			    conversion_data, result, value_indices, offset, count, row, string_payload);
		}
		case LogicalTypeId::INTERVAL:
			return CastVariantToPrimitive<VariantDirectConversion<interval_t, VariantLogicalType::INTERVAL>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::DECIMAL: {
			auto physical_type = target_type.InternalType();
			uint8_t width;
			uint8_t scale;
			target_type.GetDecimalProperties(width, scale);
			DecimalConversionPayload decimal_payload(width, scale);

			switch (physical_type) {
			case PhysicalType::INT16:
				return CastVariantToPrimitive<VariantDecimalConversion<int16_t>>(conversion_data, result, value_indices,
				                                                                 offset, count, row, decimal_payload);
			case PhysicalType::INT32:
				return CastVariantToPrimitive<VariantDecimalConversion<int32_t>>(conversion_data, result, value_indices,
				                                                                 offset, count, row, decimal_payload);
			case PhysicalType::INT64:
				return CastVariantToPrimitive<VariantDecimalConversion<int64_t>>(conversion_data, result, value_indices,
				                                                                 offset, count, row, decimal_payload);
			case PhysicalType::INT128:
				return CastVariantToPrimitive<VariantDecimalConversion<hugeint_t>>(
				    conversion_data, result, value_indices, offset, count, row, decimal_payload);
			default:
				throw NotImplementedException("Can't convert VARIANT to DECIMAL value of physical type: %s",
				                              EnumUtil::ToString(physical_type));
			};
		}
		case LogicalTypeId::TIME:
			return CastVariantToPrimitive<VariantDirectConversion<dtime_t, VariantLogicalType::TIME_MICROS>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::TIME_TZ:
			return CastVariantToPrimitive<VariantDirectConversion<dtime_tz_t, VariantLogicalType::TIME_MICROS_TZ>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::TIMESTAMP_TZ:
			return CastVariantToPrimitive<
			    VariantDirectConversion<timestamp_tz_t, VariantLogicalType::TIMESTAMP_MICROS_TZ>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::UUID:
			return CastVariantToPrimitive<VariantDirectConversion<hugeint_t, VariantLogicalType::UUID>>(
			    conversion_data, result, value_indices, offset, count, row, empty_payload);
		case LogicalTypeId::BIT:
		case LogicalTypeId::VARINT:
		default:
			error = "Can't convert VARIANT";
			return false;
		};
	}
	return true;
}

bool VariantFunctions::CastFromVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(source.GetType() == CreateVariantType());
	FromVariantConversionData conversion_data;
	Vector::RecursiveToUnifiedFormat(source, count, conversion_data.unified_format);
	auto &allocator = Allocator::DefaultAllocator();

	auto owned_value_indices = allocator.Allocate(sizeof(uint32_t) * count);
	auto value_indices = reinterpret_cast<uint32_t *>(owned_value_indices.get());
	::bzero(value_indices, sizeof(uint32_t) * count);

	auto success = CastVariant(conversion_data, result, value_indices, 0, count, optional_idx());
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	if (!success) {
		return FinalizeErrorMessage(conversion_data, result, parameters);
	}
	return true;
}

} // namespace duckdb
