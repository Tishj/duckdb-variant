#include "variant_extension.hpp"
#include "variant_functions.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

namespace duckdb {

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

//! ------- Primitive Conversion Methods -------

template <class T, VariantLogicalType TYPE_ID>
struct VariantNumericConversion {
	using type = T;
	static const constexpr VariantLogicalType type_id = TYPE_ID;
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
static T FetchVariantValue(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value) {
	if (!OP::CanConvert(type_id)) {
		throw ConversionException("Can't cast VARIANT value of type '%s' to '%s'", VariantLogicalTypeToString(type_id),
		                          VariantLogicalTypeToString(OP::type_id));
	}
	return OP::Convert(type_id, byte_offset, value);
}

template <class OP, class T = typename OP::type>
static void CastVariantToPrimitive(RecursiveUnifiedVectorFormat &variant, Vector &result, idx_t count) {
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
		auto value_index = value_format.sel->get_index(i);
		auto type_id_index = type_id_format.sel->get_index(values_list_entry.offset + i);
		auto byte_offset_index = byte_offset_format.sel->get_index(values_list_entry.offset + i);

		auto type_id = static_cast<VariantLogicalType>(type_id_data[type_id_index]);
		auto byte_offset = byte_offset_data[byte_offset_index];
		auto value_blob_data = const_data_ptr_cast(value_data[value_index].GetData());
		result_data[i] = FetchVariantValue<T, OP>(type_id, byte_offset, value_blob_data);
	}
}

bool VariantFunctions::CastFromVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(source.GetType() == CreateVariantType());
	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(source, count, source_format);

	auto &target_type = result.GetType();
	switch (target_type.id()) {
	case LogicalTypeId::INTEGER: {
		CastVariantToPrimitive<VariantNumericConversion<int32_t, VariantLogicalType::INT32>>(source_format, result,
		                                                                                     count);
		break;
	}
	case LogicalTypeId::BIGINT: {
		CastVariantToPrimitive<VariantNumericConversion<int64_t, VariantLogicalType::INT64>>(source_format, result,
		                                                                                     count);
		break;
	}
	default:
		throw NotImplementedException("Cast from VARIANT to '%s' not implemented", target_type.ToString());
	}

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

} // namespace duckdb
