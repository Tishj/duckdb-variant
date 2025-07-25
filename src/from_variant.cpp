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
	//! Mapping of struct keys -> dictionary index of the Variant 'keys'
	unordered_map<string, idx_t> mapping;
	//! The input Variant column
	RecursiveUnifiedVectorFormat unified_format;
	//! If unsuccessful - the error of the conversion
	string error;
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

static bool FinalizeErrorMessage(FromVariantConversionData &conversion_data, Vector &result, CastParameters &parameters) {
	auto conversion_error =
	    StringUtil::Format("%s to '%s'", conversion_data.error, result.GetType().ToString());
	if (parameters.error_message) {
		*parameters.error_message = conversion_error;
		return false;
	}
	throw ConversionException(conversion_error);
}

//! ------- Primitive Conversion Methods -------

struct VariantBooleanConversion {
	using type = bool;
	static bool CanConvert(const VariantLogicalType type_id) {
		return type_id == VariantLogicalType::BOOL_FALSE || type_id == VariantLogicalType::BOOL_TRUE;
	}
	static bool Convert(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value) {
		return type_id == VariantLogicalType::BOOL_TRUE;
	}
};

template <class T, VariantLogicalType TYPE_ID>
struct VariantNumericConversion {
	using type = T;
	static bool CanConvert(const VariantLogicalType type_id) {
		if (TYPE_ID == type_id) {
			//! Direct conversion always possible
			return true;
		}
		//! TODO: implement a subset of MaxLogicalType to determine whether the stored VARIANT type can be upcast to the
		//! target type.
		return false;
	}
	static T Convert(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value) {
		D_ASSERT(type_id == TYPE_ID);
		return Load<T>(value + byte_offset);
	}
};

template <class T, class OP>
static bool FetchVariantValue(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value, T &ret,
                              string &error) {
	if (!OP::CanConvert(type_id)) {
		error = StringUtil::Format("Can't convert from VARIANT(%s)", VariantLogicalTypeToString(type_id));
		return false;
	}
	ret = OP::Convert(type_id, byte_offset, value);
	return true;
}

template <class OP, class T = typename OP::type>
static bool CastVariantToPrimitive(FromVariantConversionData &conversion_data, Vector &result, uint32_t *value_indices, idx_t count) {
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
		auto &values_list_entry = values_data[values_format.sel->get_index(i)];
		auto blob_index = value_format.sel->get_index(i);

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
		if (!FetchVariantValue<T, OP>(type_id, byte_offset, value_blob_data, result_data[i], conversion_data.error)) {
			auto value = VariantConversion::ConvertVariantToValue(conversion_data.unified_format, 0, value_index);
			result.SetValue(i, value.DefaultCastAs(target_type, true));
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

static bool FindValuesWithKey(FromVariantConversionData &conversion_data, idx_t dictionary_index, optional_idx row, uint32_t *res, VariantNestedData *nested_data, idx_t count) {
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

			if (dictionary_index == key_index) {
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

static bool CastVariant(FromVariantConversionData &conversion_data, Vector &result, uint32_t *value_indices, idx_t count, optional_idx row) {
	auto &target_type = result.GetType();
	auto &allocator = Allocator::DefaultAllocator();
	auto &error = conversion_data.error;

	if (target_type.IsNested()) {
		auto owned_child_data = allocator.Allocate(sizeof(VariantNestedData) * count);
		auto child_data = reinterpret_cast<VariantNestedData *>(owned_child_data.get());

		switch (target_type.id()) {
		case LogicalTypeId::STRUCT: {
			//! First get all the Object data from the VARIANT
			if (!CollectNestedData<VariantLogicalType::OBJECT>(conversion_data, value_indices, count, row, child_data)) {
				return false;
			}

			auto &children = StructVector::GetEntries(result);
			auto &child_types = StructType::GetChildTypes(target_type);

			auto owned_value_indices = allocator.Allocate(sizeof(uint32_t) * count);
			auto new_value_indices = reinterpret_cast<uint32_t *>(owned_value_indices.get());

			for (idx_t child_idx = 0; child_idx < child_types.size(); child_idx++) {
				auto &child_name = child_types[child_idx].first;
				auto dictionary_index = conversion_data.mapping.at(child_name);

				if (!FindValuesWithKey(conversion_data, dictionary_index, row, new_value_indices, child_data, count)) {
					error = StringUtil::Format("VARIANT(OBJECT) is missing key '%s'");
					return false;
				}
				auto &child = *children[child_idx];
				if (!CastVariant(conversion_data, child, new_value_indices, count, row)) {
					return false;
				}
			}
			return true;
		}
		case LogicalTypeId::ARRAY:
		case LogicalTypeId::LIST:
		case LogicalTypeId::MAP: {
			error = "Can't convert VARIANT";
			return false;
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
		switch (target_type.id()) {
		case LogicalTypeId::BOOLEAN:
			return CastVariantToPrimitive<VariantBooleanConversion>(conversion_data, result, value_indices, count);
		case LogicalTypeId::TINYINT:
			return CastVariantToPrimitive<VariantNumericConversion<int8_t, VariantLogicalType::INT8>>(conversion_data, result, value_indices,
			                                                                                          count);
		case LogicalTypeId::SMALLINT:
			return CastVariantToPrimitive<VariantNumericConversion<int16_t, VariantLogicalType::INT16>>(conversion_data, result, value_indices,
			                                                                                            count);
		case LogicalTypeId::INTEGER:
			return CastVariantToPrimitive<VariantNumericConversion<int32_t, VariantLogicalType::INT32>>(conversion_data, result, value_indices,
			                                                                                            count);
		case LogicalTypeId::BIGINT:
			return CastVariantToPrimitive<VariantNumericConversion<int64_t, VariantLogicalType::INT64>>(conversion_data, result, value_indices,
			                                                                                            count);
		case LogicalTypeId::HUGEINT:
			return CastVariantToPrimitive<VariantNumericConversion<hugeint_t, VariantLogicalType::INT128>>(
			    conversion_data, result, value_indices, count);
		case LogicalTypeId::UTINYINT:
			return CastVariantToPrimitive<VariantNumericConversion<uint8_t, VariantLogicalType::UINT8>>(conversion_data, result, value_indices,
			                                                                                            count);
		case LogicalTypeId::USMALLINT:
			return CastVariantToPrimitive<VariantNumericConversion<uint16_t, VariantLogicalType::UINT16>>(
			    conversion_data, result, value_indices, count);
		case LogicalTypeId::UINTEGER:
			return CastVariantToPrimitive<VariantNumericConversion<uint32_t, VariantLogicalType::UINT32>>(
			    conversion_data, result, value_indices, count);
		case LogicalTypeId::UBIGINT:
			return CastVariantToPrimitive<VariantNumericConversion<uint64_t, VariantLogicalType::UINT64>>(
			    conversion_data, result, value_indices, count);
		case LogicalTypeId::UHUGEINT:
			return CastVariantToPrimitive<VariantNumericConversion<uhugeint_t, VariantLogicalType::UINT128>>(
			    conversion_data, result, value_indices, count);
		case LogicalTypeId::FLOAT:
			return CastVariantToPrimitive<VariantNumericConversion<float, VariantLogicalType::FLOAT>>(conversion_data, result, value_indices,
			                                                                                          count);
		case LogicalTypeId::DOUBLE:
			return CastVariantToPrimitive<VariantNumericConversion<double, VariantLogicalType::DOUBLE>>(conversion_data, result, value_indices,
			                                                                                            count);
		default:
			error = "Can't convert VARIANT";
			return false;
		};
	}
	return true;
}

static bool AddToMapping(Vector &dictionary, idx_t dictionary_size, const string &key,
                         unordered_map<string, idx_t> &mapping) {
	if (mapping.count(key)) {
		return true;
	}

	auto dictionary_data = FlatVector::GetData<string_t>(dictionary);
	string_t child_name_str(key.c_str(), key.size());

	// Binary search in sorted dictionary
	idx_t left = 0, right = dictionary_size;
	while (left < right) {
		idx_t mid = left + (right - left) / 2;
		if (dictionary_data[mid] == child_name_str) {
			mapping.emplace(key, mid);
			return true;
		} else if (dictionary_data[mid] < child_name_str) {
			left = mid + 1;
		} else {
			right = mid;
		}
	}

	// Key not found
	return false;
}

bool PopulateDictionaryMapping(Vector &source, FromVariantConversionData &conversion_data, const LogicalType &target_type, idx_t count) {
	auto &keys_entry = ListVector::GetEntry(VariantVector::GetKeys(source));

	reference<Vector> dictionary(keys_entry);
	idx_t dictionary_size = count;
	if (keys_entry.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		dictionary = DictionaryVector::Child(keys_entry);
		auto opt_dictionary_size = DictionaryVector::DictionarySize(keys_entry);
		if (!opt_dictionary_size.IsValid()) {
			return true;
		}
		dictionary_size = opt_dictionary_size.GetIndex();
	}

	auto &error = conversion_data.error;
	bool success = true;
	//! struct key -> mapping
	//! With this mapping we can look up the mapping for the struct key (which is guaranteed to be present)
	//! Then use `key_entry_format.sel->get_index(key_id)` to get the dictionary index for a given child.
	//! Which we can then compare to the dictionary index we looked up from 'mapping'
	auto &mapping = conversion_data.mapping;
	TypeVisitor::Contains(
	    target_type, [&mapping, &dictionary, dictionary_size, &success, &error](const LogicalType &type) {
		    if (type.InternalType() == PhysicalType::STRUCT) {
			    auto &children = StructType::GetChildTypes(type);
			    for (auto &child : children) {
				    if (!AddToMapping(dictionary, dictionary_size, child.first, mapping)) {
					    error = StringUtil::Format("Struct key '%s' is missing from VARIANT", child.first);
					    success = false;
					    return false;
				    }
			    }
		    } else if (type.id() == LogicalTypeId::MAP) {
			    if (!AddToMapping(dictionary, dictionary_size, "key", mapping)) {
				    error = StringUtil::Format("Struct key '%s' is missing from VARIANT", "key");
				    success = false;
				    return false;
			    }
			    if (!AddToMapping(dictionary, dictionary_size, "value", mapping)) {
				    error = StringUtil::Format("Struct key '%s' is missing from VARIANT", "value");
				    success = false;
				    return false;
			    }
		    }
		    return false;
	    });
	return success;
}

bool VariantFunctions::CastFromVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(source.GetType() == CreateVariantType());
	FromVariantConversionData conversion_data;
	Vector::RecursiveToUnifiedFormat(source, count, conversion_data.unified_format);
	auto &allocator = Allocator::DefaultAllocator();

	auto &target_type = result.GetType();
	auto success = PopulateDictionaryMapping(source, conversion_data, target_type, count);
	if (!success) {
		return FinalizeErrorMessage(conversion_data, result, parameters);
	}

	auto owned_value_indices = allocator.Allocate(sizeof(uint32_t) * count);
	auto value_indices = reinterpret_cast<uint32_t *>(owned_value_indices.get());
	::bzero(value_indices, sizeof(uint32_t) * count);

	success = CastVariant(conversion_data, result, value_indices, count, optional_idx());
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	if (!success) {
		return FinalizeErrorMessage(conversion_data, result, parameters);
	}
	return true;
}

} // namespace duckdb
