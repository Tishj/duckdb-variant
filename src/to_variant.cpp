#include "variant_functions.hpp"
#include "variant_extension.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

namespace {

template <class T>
static uint8_t GetVarintSize(T val) {
	uint8_t res = 0;
	do {
		val >>= 7;
		res++;
	} while (val != 0);
	return res;
}

template <class T>
static void VarintEncode(T val, data_ptr_t ptr) {
	do {
		uint8_t byte = val & 127;
		val >>= 7;
		if (val != 0) {
			byte |= 128;
		}
		*ptr = byte;
		ptr++;
	} while (val != 0);
}

struct OffsetData {
	static uint32_t *GetKeys(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(offsets.data[0]);
	}
	static uint32_t *GetChildren(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(offsets.data[1]);
	}
	static uint32_t *GetValues(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(offsets.data[2]);
	}
	static uint32_t *GetBlob(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(offsets.data[3]);
	}
};

struct StringDictionary {
public:
	void AddString(Vector &vec, string_t str) {
		auto it = dictionary.find(str);

		if (it != dictionary.end()) {
			return;
		}
		auto vec_data = FlatVector::GetData<string_t>(vec);
		auto dict_count = dictionary.size();
		if (dict_count >= dictionary_capacity) {
			auto new_capacity = NextPowerOfTwo(dictionary_capacity + 1);
			vec.Resize(dictionary_capacity, new_capacity);
			dictionary_capacity = new_capacity;
		}
		vec_data[dict_count] = StringVector::AddStringOrBlob(vec, std::move(str));
		dictionary.emplace(vec_data[dict_count], dict_count);
	}
	idx_t Find(string_t str) {
		auto it = dictionary.find(str);
		//! The dictionary was populated in the first pass, looked up in the second
		D_ASSERT(it != dictionary.end());
		return it->second;
	}

public:
	//! Ensure uniqueness of the dictionary entries
	string_map_t<idx_t> dictionary;
	idx_t dictionary_capacity = STANDARD_VECTOR_SIZE;
};

struct VariantVectorData {
public:
	explicit VariantVectorData(Vector &variant)
	    : key_id_validity(FlatVector::Validity(VariantVector::GetChildrenKeyId(variant))) {
		blob_data = FlatVector::GetData<string_t>(VariantVector::GetValue(variant));
		type_ids_data = FlatVector::GetData<uint8_t>(VariantVector::GetValuesTypeId(variant));
		byte_offset_data = FlatVector::GetData<uint32_t>(VariantVector::GetValuesByteOffset(variant));
		key_id_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenKeyId(variant));
		value_id_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenValueId(variant));
		values_data = FlatVector::GetData<list_entry_t>(VariantVector::GetValues(variant));
		children_data = FlatVector::GetData<list_entry_t>(VariantVector::GetChildren(variant));
		keys_data = FlatVector::GetData<list_entry_t>(VariantVector::GetKeys(variant));
	}

public:
	//! value
	string_t *blob_data;

	//! values
	uint8_t *type_ids_data;
	uint32_t *byte_offset_data;

	//! children
	uint32_t *key_id_data;
	uint32_t *value_id_data;
	ValidityMask &key_id_validity;

	//! values | children | keys
	list_entry_t *values_data;
	list_entry_t *children_data;
	list_entry_t *keys_data;
};

} // namespace

//! -------- Determine the 'type_id' for the Value --------

template <typename T, VariantLogicalType TYPE_ID>
static VariantLogicalType GetTypeId(T val) {
	return TYPE_ID;
}

template <>
VariantLogicalType GetTypeId<bool, VariantLogicalType::VARIANT_NULL>(bool val) {
	return val ? VariantLogicalType::BOOL_TRUE : VariantLogicalType::BOOL_FALSE;
}

//! -------- Write the 'value' data for the Value --------

template <typename T>
static void WriteData(data_ptr_t ptr, const T &val, vector<idx_t> &lengths) {
	Store(val, ptr);
}
template <>
void WriteData(data_ptr_t ptr, const bool &val, vector<idx_t> &lengths) {
	return;
}
template <>
void WriteData(data_ptr_t ptr, const string_t &val, vector<idx_t> &lengths) {
	D_ASSERT(lengths.size() == 2);
	auto str_length = val.GetSize();
	VarintEncode(str_length, ptr);
	memcpy(ptr + lengths[0], val.GetData(), str_length);
}

//! decimal

template <typename T>
static void WriteDecimalData(data_ptr_t ptr, const T &val, vector<idx_t> &lengths, idx_t width, idx_t scale) {
	throw InternalException("WriteDecimalData not implemented for this type");
}
template <>
void WriteDecimalData(data_ptr_t ptr, const int16_t &val, vector<idx_t> &lengths, idx_t width, idx_t scale) {
	D_ASSERT(lengths.size() == 3);
	VarintEncode(width, ptr);
	VarintEncode(scale, ptr + lengths[0]);
	Store(val, ptr + lengths[0] + lengths[1]);
}
template <>
void WriteDecimalData(data_ptr_t ptr, const int32_t &val, vector<idx_t> &lengths, idx_t width, idx_t scale) {
	D_ASSERT(lengths.size() == 3);
	VarintEncode(width, ptr);
	VarintEncode(scale, ptr + lengths[0]);
	Store(val, ptr + lengths[0] + lengths[1]);
}
template <>
void WriteDecimalData(data_ptr_t ptr, const int64_t &val, vector<idx_t> &lengths, idx_t width, idx_t scale) {
	D_ASSERT(lengths.size() == 3);
	VarintEncode(width, ptr);
	VarintEncode(scale, ptr + lengths[0]);
	Store(val, ptr + lengths[0] + lengths[1]);
}
template <>
void WriteDecimalData(data_ptr_t ptr, const hugeint_t &val, vector<idx_t> &lengths, idx_t width, idx_t scale) {
	D_ASSERT(lengths.size() == 3);
	VarintEncode(width, ptr);
	VarintEncode(scale, ptr + lengths[0]);
	Store(val, ptr + lengths[0] + lengths[1]);
}

//! -------- Determine size of the 'value' data for the Value --------

template <typename T>
static void GetValueSize(const T &val, vector<idx_t> &lengths) {
	lengths.push_back(sizeof(T));
}
template <>
void GetValueSize(const bool &val, vector<idx_t> &lengths) {
}
template <>
void GetValueSize(const string_t &val, vector<idx_t> &lengths) {
	auto value_size = val.GetSize();
	lengths.push_back(GetVarintSize(value_size));
	lengths.push_back(value_size);
}

//! decimal

template <typename T>
static void GetDecimalValueSize(const T &val, vector<idx_t> &lengths, idx_t width, idx_t scale) {
	throw InternalException("GetDecimalValueSize not implemented for this type");
}
template <>
void GetDecimalValueSize(const int16_t &val, vector<idx_t> &lengths, idx_t width, idx_t scale) {
	lengths.push_back(GetVarintSize(width));
	lengths.push_back(GetVarintSize(scale));
	lengths.push_back(sizeof(int16_t));
}
template <>
void GetDecimalValueSize(const int32_t &val, vector<idx_t> &lengths, idx_t width, idx_t scale) {
	lengths.push_back(GetVarintSize(width));
	lengths.push_back(GetVarintSize(scale));
	lengths.push_back(sizeof(int16_t));
}
template <>
void GetDecimalValueSize(const int64_t &val, vector<idx_t> &lengths, idx_t width, idx_t scale) {
	lengths.push_back(GetVarintSize(width));
	lengths.push_back(GetVarintSize(scale));
	lengths.push_back(sizeof(int16_t));
}
template <>
void GetDecimalValueSize(const hugeint_t &val, vector<idx_t> &lengths, idx_t width, idx_t scale) {
	lengths.push_back(GetVarintSize(width));
	lengths.push_back(GetVarintSize(scale));
	lengths.push_back(sizeof(hugeint_t));
}

//! -------- Convert primitive values to Variant --------

template <bool WRITE_DATA, VariantLogicalType TYPE_ID, class T>
static bool ConvertPrimitiveToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                                      SelectionVector *selvec, SelectionVector *value_ids_selvec) {
	const auto &type = source.GetType();

	auto blob_offset_data = OffsetData::GetBlob(offsets);
	auto values_offset_data = OffsetData::GetValues(offsets);

	//! Values only used for Decimal conversion
	uint8_t width;
	uint8_t scale;
	if (TYPE_ID == VariantLogicalType::DECIMAL) {
		type.GetDecimalProperties(width, scale);
	}

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	auto source_data = source_format.GetData<T>(source_format);
	for (idx_t i = 0; i < count; i++) {
		auto index = source_format.sel->get_index(i);

		auto result_index = selvec ? selvec->get_index(i) : i;
		auto &blob_offset = blob_offset_data[result_index];
		auto &values_list_entry = result.values_data[result_index];

		auto &val = source_data[index];
		vector<idx_t> lengths;
		if (TYPE_ID == VariantLogicalType::DECIMAL) {
			GetDecimalValueSize<T>(val, lengths, width, scale);
		} else {
			GetValueSize<T>(val, lengths);
		}

		if (WRITE_DATA) {
			auto &blob_value = result.blob_data[result_index];
			auto blob_value_data = data_ptr_cast(blob_value.GetDataWriteable());

			auto values_offset = values_list_entry.offset + values_offset_data[result_index];
			result.type_ids_data[values_offset] = static_cast<uint8_t>(GetTypeId<T, TYPE_ID>(val));
			result.byte_offset_data[values_offset] = blob_offset;
			if (value_ids_selvec) {
				//! Set for the parent where this child lives in the 'values' list
				result.value_id_data[value_ids_selvec->get_index(i)] = values_offset_data[result_index];
			}
			if (TYPE_ID == VariantLogicalType::DECIMAL) {
				WriteDecimalData<T>(blob_value_data + blob_offset, val, lengths, width, scale);
			} else {
				WriteData<T>(blob_value_data + blob_offset, val, lengths);
			}
		}
		for (auto &length : lengths) {
			blob_offset += length;
		}
		values_offset_data[result_index]++;
	}
	return true;
}

//! * @param source The Vector of arbitrary type to process
//! * @param result The result Vector to write the variant data to
//! * @param offsets The offsets to gather per row
//! * @param count The size of the source vector
//! * @param selvec The selection vector from i (< count) to the index in the offsets Vectors
//! * @param keys_selvec The selection vector to populate with mapping from keys index -> dictionary index
//! * @param dictionary The dictionary to look up the dictionary index from
//! * @param value_ids_selvec The selection vector from i (< count) to the index in the children.value_ids selvec, to
//! populate the parent's children
template <bool WRITE_DATA>
static bool ConvertToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                             SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary,
                             SelectionVector *value_ids_selvec) {
	auto &type = source.GetType();

	auto physical_type = type.InternalType();
	auto logical_type = type.id();
	auto blob_offset_data = OffsetData::GetBlob(offsets);
	auto values_offset_data = OffsetData::GetValues(offsets);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	if (type.IsNested()) {
		auto keys_offset_data = OffsetData::GetKeys(offsets);
		auto children_offset_data = OffsetData::GetChildren(offsets);
		//! FIXME: use logical type instead
		if (physical_type == PhysicalType::LIST) {
			auto source_data = source_format.GetData<list_entry_t>(source_format);

			auto list_size = ListVector::GetListSize(source);
			//! Create a selection vector that maps to the right row in the 'result' for the child vector
			SelectionVector new_selection(0, list_size);
			//! Create a selection vector that maps to the children.value_id entry of the parent
			SelectionVector children_selection(0, list_size);
			for (idx_t i = 0; i < count; i++) {
				const auto index = source_format.sel->get_index(i);
				const auto result_index = selvec ? selvec->get_index(i) : i;
				auto &entry = source_data[index];

				auto &blob_offset = blob_offset_data[result_index];
				auto &values_list_entry = result.values_data[result_index];
				auto &children_list_entry = result.children_data[result_index];

				//! values
				if (WRITE_DATA) {
					//! type_id + byte_offset
					auto values_offset = values_list_entry.offset + values_offset_data[result_index];
					result.type_ids_data[values_offset] = static_cast<uint8_t>(VariantLogicalType::ARRAY);
					result.byte_offset_data[values_offset] = blob_offset;
					if (value_ids_selvec) {
						//! Set for the parent where this child lives in the 'values' list
						result.value_id_data[value_ids_selvec->get_index(i)] = values_offset_data[result_index];
					}
				}
				values_offset_data[result_index]++;

				//! value
				const auto length_varint_size = GetVarintSize(entry.length);
				const auto child_offset_varint_size =
				    length_varint_size ? GetVarintSize(children_offset_data[result_index]) : 0;
				if (WRITE_DATA) {
					auto &blob_value = result.blob_data[result_index];
					auto blob_value_data = data_ptr_cast(blob_value.GetDataWriteable());

					VarintEncode(entry.length, blob_value_data + blob_offset);
					if (entry.length) {
						VarintEncode(children_offset_data[result_index],
						             blob_value_data + blob_offset + length_varint_size);
					}
				}
				blob_offset += length_varint_size + child_offset_varint_size;

				auto children_offset = children_list_entry.offset + children_offset_data[result_index];
				for (idx_t child_idx = 0; child_idx < entry.length; child_idx++) {
					//! Set up the selection vector for the child of the list vector
					new_selection.set_index(child_idx + entry.offset, result_index);
					if (WRITE_DATA) {
						children_selection.set_index(child_idx + entry.offset, children_offset + child_idx);
						result.key_id_validity.SetInvalid(children_offset + child_idx);
					}
				}

				//! children
				children_offset_data[result_index] += entry.length;
			}
			//! Now write the child vector of the list (for all rows)
			auto &entry = ListVector::GetEntry(source);
			ConvertToVariant<WRITE_DATA>(entry, result, offsets, list_size, &new_selection, keys_selvec, dictionary,
			                             &children_selection);
		} else if (physical_type == PhysicalType::STRUCT) {
			auto &children = StructVector::GetEntries(source);
			auto &struct_children = StructType::GetChildTypes(type);

			//! Look up all the dictionary indices for the struct keys
			vector<uint32_t> dictionary_indices(children.size());
			if (WRITE_DATA) {
				for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
					auto &struct_child = struct_children[child_idx];
					string_t struct_child_str(struct_child.first.c_str(), struct_child.first.size());
					dictionary_indices[child_idx] = dictionary.Find(std::move(struct_child_str));
				}
			}

			SelectionVector children_selection(0, count);
			for (idx_t i = 0; i < count; i++) {
				auto result_index = selvec ? selvec->get_index(i) : i;

				auto &blob_offset = blob_offset_data[result_index];
				auto &values_list_entry = result.values_data[result_index];
				auto &children_list_entry = result.children_data[result_index];
				auto &keys_list_entry = result.keys_data[result_index];

				//! values
				if (WRITE_DATA) {
					//! type_id + byte_offset
					auto values_offset = values_list_entry.offset + values_offset_data[result_index];
					result.type_ids_data[values_offset] = static_cast<uint8_t>(VariantLogicalType::OBJECT);
					result.byte_offset_data[values_offset] = blob_offset;
					if (value_ids_selvec) {
						//! Set for the parent where this child lives in the 'values' list
						result.value_id_data[value_ids_selvec->get_index(i)] = values_offset_data[result_index];
					}
				}
				values_offset_data[result_index]++;

				//! value
				const auto length_varint_size = GetVarintSize(children.size());
				const auto child_offset_varint_size = GetVarintSize(children_offset_data[result_index]);
				if (WRITE_DATA) {
					auto &blob_value = result.blob_data[result_index];
					auto blob_value_data = data_ptr_cast(blob_value.GetDataWriteable());

					VarintEncode(children.size(), blob_value_data + blob_offset);
					VarintEncode(children_offset_data[result_index],
					             blob_value_data + blob_offset + length_varint_size);
				}
				blob_offset += length_varint_size + child_offset_varint_size;

				//! children
				if (WRITE_DATA) {
					auto children_offset = children_list_entry.offset + children_offset_data[result_index];
					auto keys_offset = keys_list_entry.offset + keys_offset_data[result_index];
					for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
						result.key_id_data[children_offset + child_idx] = keys_offset_data[result_index] + child_idx;
						keys_selvec.set_index(keys_offset + child_idx, dictionary_indices[child_idx]);
					}
					//! Map from index of the child to the children.value_ids of the parent
					children_selection.set_index(i, children_offset);
				}

				children_offset_data[result_index] += children.size();
				keys_offset_data[result_index] += children.size();
			}

			for (auto &child_ptr : children) {
				auto &child = *child_ptr;

				ConvertToVariant<WRITE_DATA>(child, result, offsets, count, selvec, keys_selvec, dictionary,
				                             &children_selection);
				if (WRITE_DATA) {
					//! Now forward the selection to point to the next index in the children.value_ids
					for (idx_t i = 0; i < count; i++) {
						children_selection[i]++;
					}
				}
			}
		} else {
			throw NotImplementedException("Can't convert nested physical type '%s'", EnumUtil::ToString(physical_type));
		}
	} else {
		switch (type.id()) {
		case LogicalTypeId::BOOLEAN:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::VARIANT_NULL, bool>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::TINYINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::INT8, int8_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::UTINYINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::UINT8, uint8_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::SMALLINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::INT16, int16_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::USMALLINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::UINT16, uint16_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::INTEGER:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::INT32, int32_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::UINTEGER:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::UINT32, uint32_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::BIGINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::INT64, int64_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::UBIGINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::UINT64, uint64_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::HUGEINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::INT128, hugeint_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::UHUGEINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::UINT128, uhugeint_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::DATE:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::DATE, date_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::TIME:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::TIME_MICROS, dtime_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::TIME_NS:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::TIME_NANOS, dtime_ns_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::TIMESTAMP:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::TIMESTAMP_MICROS, timestamp_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::TIMESTAMP_SEC:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::TIMESTAMP_SEC, timestamp_sec_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::TIMESTAMP_NS:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::TIMESTAMP_NANOS, timestamp_ns_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::TIMESTAMP_MS:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::TIMESTAMP_MILIS, timestamp_ms_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::TIME_TZ:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::TIME_MICROS_TZ, dtime_tz_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::TIMESTAMP_TZ:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::TIMESTAMP_MICROS_TZ, timestamp_tz_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::UUID:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::UUID, hugeint_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::FLOAT:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::FLOAT, float>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::DOUBLE:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::DOUBLE, double>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::DECIMAL: {
			switch (physical_type) {
			case PhysicalType::INT16:
				return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::DECIMAL, int16_t>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::INT32:
				return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::DECIMAL, int32_t>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::INT64:
				return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::DECIMAL, int64_t>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::INT128:
				return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::DECIMAL, hugeint_t>(source, result, offsets, count, selvec, value_ids_selvec);
			default:
				throw NotImplementedException("Can't convert DECIMAL value of physical type: %s", EnumUtil::ToString(physical_type));
			};
		}
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::CHAR:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::VARCHAR, string_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::BLOB:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::BLOB, string_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::INTERVAL:
			return ConvertPrimitiveToVariant<WRITE_DATA, VariantLogicalType::INTERVAL, interval_t>(source, result, offsets, count, selvec, value_ids_selvec);
		case LogicalTypeId::ENUM:
			throw NotImplementedException("ENUM conversion not supported");
		case LogicalTypeId::BIT:
		case LogicalTypeId::VARINT:
		default:
			throw NotImplementedException("Invalid PhysicalType (%s) for ConvertToVariant", EnumUtil::ToString(physical_type));
		}
	}
	return true;
}

static void InitializeOffsets(DataChunk &offsets, idx_t count) {
	auto keys = OffsetData::GetKeys(offsets);
	auto children = OffsetData::GetChildren(offsets);
	auto values = OffsetData::GetValues(offsets);
	auto blob = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < count; i++) {
		keys[i] = 0;
		children[i] = 0;
		values[i] = 0;
		blob[i] = 0;
	}
}

static void InitializeVariants(DataChunk &offsets, Vector &result, SelectionVector &keys_selvec, idx_t &selvec_size) {
	auto &keys = VariantVector::GetKeys(result);
	auto keys_data = ListVector::GetData(keys);

	auto &children = VariantVector::GetChildren(result);
	auto children_data = ListVector::GetData(children);

	auto &values = VariantVector::GetValues(result);
	auto values_data = ListVector::GetData(values);

	auto &blob = VariantVector::GetValue(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);

	idx_t keys_offset = 0;
	idx_t children_offset = 0;
	idx_t values_offset = 0;

	auto keys_sizes = OffsetData::GetKeys(offsets);
	auto children_sizes = OffsetData::GetChildren(offsets);
	auto values_sizes = OffsetData::GetValues(offsets);
	auto blob_sizes = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < offsets.size(); i++) {
		auto &keys_entry = keys_data[i];
		auto &children_entry = children_data[i];
		auto &values_entry = values_data[i];

		//! keys
		keys_entry.length = keys_sizes[i];
		keys_entry.offset = keys_offset;
		keys_offset += keys_entry.length;

		//! children
		children_entry.length = children_sizes[i];
		children_entry.offset = children_offset;
		children_offset += children_entry.length;

		//! values
		values_entry.length = values_sizes[i];
		values_entry.offset = values_offset;
		values_offset += values_entry.length;

		//! value
		blob_data[i] = StringVector::EmptyString(blob, blob_sizes[i]);
	}
	//! Reserve for the children of the lists
	ListVector::Reserve(keys, keys_offset);
	ListVector::Reserve(children, children_offset);
	ListVector::Reserve(values, values_offset);

	//! Set list sizes
	ListVector::SetListSize(keys, keys_offset);
	ListVector::SetListSize(children, children_offset);
	ListVector::SetListSize(values, values_offset);

	keys_selvec.Initialize(keys_offset);
	selvec_size = keys_offset;
}

bool VariantFunctions::CastToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	DataChunk offsets;
	offsets.Initialize(Allocator::DefaultAllocator(),
	                   {LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER},
	                   count);
	offsets.SetCardinality(count);

	//! Initialize the dictionary
	StringDictionary dictionary;
	auto &keys_entry = ListVector::GetEntry(VariantVector::GetKeys(result));
	TypeVisitor::Contains(source.GetType(), [&dictionary, &keys_entry](const LogicalType &type) {
		if (type.InternalType() == PhysicalType::STRUCT) {
			auto &children = StructType::GetChildTypes(type);
			for (auto &child : children) {
				string_t child_name_str(child.first.c_str(), child.first.size());
				dictionary.AddString(keys_entry, std::move(child_name_str));
			}
		}
		return false;
	});
	SelectionVector keys_selvec;

	{
		VariantVectorData result_data(result);
		//! First pass - collect sizes/offsets
		InitializeOffsets(offsets, count);
		ConvertToVariant<false>(source, result_data, offsets, count, nullptr, keys_selvec, dictionary, nullptr);
	}

	//! This resizes the lists, invalidating the "GetData" results stored in VariantVectorData
	idx_t keys_selvec_size;
	InitializeVariants(offsets, result, keys_selvec, keys_selvec_size);

	{
		VariantVectorData result_data(result);
		//! Second pass - actually construct the variants
		InitializeOffsets(offsets, count);
		ConvertToVariant<true>(source, result_data, offsets, count, nullptr, keys_selvec, dictionary, nullptr);
	}

	keys_entry.Slice(keys_selvec, keys_selvec_size);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

} // namespace duckdb
