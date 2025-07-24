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
static bool CastVariantToPrimitive(RecursiveUnifiedVectorFormat &variant, Vector &result, idx_t count, string &error) {
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
		if (!FetchVariantValue<T, OP>(type_id, byte_offset, value_blob_data, result_data[i], error)) {
			return false;
		}
	}
	return true;
}

static bool CastVariant(RecursiveUnifiedVectorFormat &variant, Vector &result, idx_t count, string &error) {
	auto &target_type = result.GetType();
	switch (target_type.id()) {
	case LogicalTypeId::BOOLEAN:
		return CastVariantToPrimitive<VariantBooleanConversion>(variant, result, count, error);
	case LogicalTypeId::TINYINT:
		return CastVariantToPrimitive<VariantNumericConversion<int8_t, VariantLogicalType::INT8>>(variant, result,
		                                                                                          count, error);
	case LogicalTypeId::SMALLINT:
		return CastVariantToPrimitive<VariantNumericConversion<int16_t, VariantLogicalType::INT16>>(variant, result,
		                                                                                            count, error);
	case LogicalTypeId::INTEGER:
		return CastVariantToPrimitive<VariantNumericConversion<int32_t, VariantLogicalType::INT32>>(variant, result,
		                                                                                            count, error);
	case LogicalTypeId::BIGINT:
		return CastVariantToPrimitive<VariantNumericConversion<int64_t, VariantLogicalType::INT64>>(variant, result,
		                                                                                            count, error);
	case LogicalTypeId::HUGEINT:
		return CastVariantToPrimitive<VariantNumericConversion<hugeint_t, VariantLogicalType::INT128>>(variant, result,
		                                                                                               count, error);
	case LogicalTypeId::UTINYINT:
		return CastVariantToPrimitive<VariantNumericConversion<uint8_t, VariantLogicalType::UINT8>>(variant, result,
		                                                                                            count, error);
	case LogicalTypeId::USMALLINT:
		return CastVariantToPrimitive<VariantNumericConversion<uint16_t, VariantLogicalType::UINT16>>(variant, result,
		                                                                                              count, error);
	case LogicalTypeId::UINTEGER:
		return CastVariantToPrimitive<VariantNumericConversion<uint32_t, VariantLogicalType::UINT32>>(variant, result,
		                                                                                              count, error);
	case LogicalTypeId::UBIGINT:
		return CastVariantToPrimitive<VariantNumericConversion<uint64_t, VariantLogicalType::UINT64>>(variant, result,
		                                                                                              count, error);
	case LogicalTypeId::UHUGEINT:
		return CastVariantToPrimitive<VariantNumericConversion<uhugeint_t, VariantLogicalType::UINT128>>(
		    variant, result, count, error);
	case LogicalTypeId::FLOAT:
		return CastVariantToPrimitive<VariantNumericConversion<float, VariantLogicalType::FLOAT>>(variant, result,
		                                                                                          count, error);
	case LogicalTypeId::DOUBLE:
		return CastVariantToPrimitive<VariantNumericConversion<double, VariantLogicalType::DOUBLE>>(variant, result,
		                                                                                            count, error);
	default:
		return false;
	}
}

bool VariantFunctions::CastFromVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(source.GetType() == CreateVariantType());
	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(source, count, source_format);

	string error;
	auto success = CastVariant(source_format, result, count, error);
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	if (!success) {
		auto conversion_error = StringUtil::Format("%s to '%s'", error, result.GetType().ToString());
		if (parameters.error_message) {
			*parameters.error_message = conversion_error;
			return false;
		}
		throw ConversionException(conversion_error);
	}
	return true;
}

} // namespace duckdb
