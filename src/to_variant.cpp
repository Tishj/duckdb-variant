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

} // namespace

VariantLogicalType LogicalTypeToVariant(LogicalTypeId type) {
	switch (type) {
	case LogicalTypeId::BOOLEAN: {
		//! This won't be used directly anyway
		return VariantLogicalType::BOOL_TRUE;
	}
	case LogicalTypeId::TINYINT:
		return VariantLogicalType::INT8;
	case LogicalTypeId::UTINYINT:
		return VariantLogicalType::UINT8;
	case LogicalTypeId::SMALLINT:
		return VariantLogicalType::INT16;
	case LogicalTypeId::USMALLINT:
		return VariantLogicalType::UINT16;
	case LogicalTypeId::SQLNULL:
		return VariantLogicalType::VARIANT_NULL;
	case LogicalTypeId::DATE:
		return VariantLogicalType::DATE;
	case LogicalTypeId::INTEGER:
		return VariantLogicalType::INT32;
	case LogicalTypeId::UINTEGER:
		return VariantLogicalType::UINT32;
	case LogicalTypeId::BIGINT:
		return VariantLogicalType::INT64;
	case LogicalTypeId::TIME:
		return VariantLogicalType::TIME;
	case LogicalTypeId::TIMESTAMP:
		return VariantLogicalType::TIMESTAMP_MICROS;
	case LogicalTypeId::TIMESTAMP_SEC:
		return VariantLogicalType::TIMESTAMP_SEC;
	case LogicalTypeId::TIMESTAMP_NS:
		return VariantLogicalType::TIMESTAMP_NANOS;
	case LogicalTypeId::TIMESTAMP_MS:
		return VariantLogicalType::TIMESTAMP_MILIS;
	case LogicalTypeId::TIME_TZ:
		return VariantLogicalType::TIME_TZ;
	case LogicalTypeId::TIMESTAMP_TZ:
		return VariantLogicalType::TIMESTAMP_MICROS_TZ;
	case LogicalTypeId::UBIGINT:
		return VariantLogicalType::UINT64;
	case LogicalTypeId::UHUGEINT:
		return VariantLogicalType::UINT128;
	case LogicalTypeId::HUGEINT:
		return VariantLogicalType::INT128;
	case LogicalTypeId::UUID:
		return VariantLogicalType::UUID;
	case LogicalTypeId::FLOAT:
		return VariantLogicalType::FLOAT;
	case LogicalTypeId::DOUBLE:
		return VariantLogicalType::DOUBLE;
	case LogicalTypeId::DECIMAL:
		return VariantLogicalType::DECIMAL;
	case LogicalTypeId::VARCHAR:
		return VariantLogicalType::VARCHAR;
	case LogicalTypeId::BLOB:
		return VariantLogicalType::BLOB;
	case LogicalTypeId::INTERVAL:
		return VariantLogicalType::INTERVAL;
	default:
		throw NotImplementedException("Can't map from LogicalTypeId (%s) to VariantLogicalType", EnumUtil::ToString(type));
	}
}

static VariantLogicalType GetTypeId(bool val, const VariantLogicalType ) {
	return val ? VariantLogicalType::BOOL_TRUE : VariantLogicalType::BOOL_FALSE;
}

template <typename T>
static VariantLogicalType GetTypeId(T val, const VariantLogicalType type) {
	return type;
}

template <bool WRITE_DATA, class T, bool IS_BOOL = false>
static bool ConvertPrimitiveToVariant(Vector &source, Vector &result, DataChunk &offsets, idx_t count, SelectionVector *selvec, SelectionVector *value_ids_selvec) {
	const auto &type = source.GetType();
	auto variant_type = LogicalTypeToVariant(type.id());

	auto blob_offset_data = OffsetData::GetBlob(offsets);
	auto values_offset_data = OffsetData::GetChildren(offsets);

	//! value
	auto &blob = VariantVector::GetValue(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);

	//! values
	auto &values = VariantVector::GetValues(result);
	auto values_data = ListVector::GetData(values);

	//! type_ids
	auto &type_ids = VariantVector::GetValuesTypeId(result);
	auto type_ids_data = FlatVector::GetData<uint8_t>(type_ids);

	//! byte_offset
	auto &byte_offset = VariantVector::GetValuesByteOffset(result);
	auto byte_offset_data = FlatVector::GetData<uint32_t>(byte_offset);

	//! value_id
	auto &value_id = VariantVector::GetChildrenValueId(result);
	auto value_id_data = FlatVector::GetData<uint32_t>(value_id);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	auto source_data = source_format.GetData<T>(source_format);
	for (idx_t i = 0; i < count; i++) {
		auto index = source_format.sel->get_index(i);

		auto result_index = selvec ? selvec->get_index(i) : i;
		auto &blob_offset = blob_offset_data[result_index];
		auto &values_list_entry = values_data[result_index];

		if (WRITE_DATA) {
			auto val = source_data[index];

			auto &blob_value = blob_data[result_index];
			auto blob_value_data = data_ptr_cast(blob_value.GetDataWriteable());

			auto values_offset = values_list_entry.offset + values_offset_data[result_index];
			type_ids_data[values_offset] = static_cast<uint8_t>(GetTypeId(val, variant_type));
			if (!IS_BOOL) {
				Store(val, blob_value_data + blob_offset);
			}
			byte_offset_data[values_offset] = blob_offset;
			if (value_ids_selvec) {
				//! Set for the parent where this child lives in the 'values' list
				value_id_data[value_ids_selvec->get_index(i)] = values_offset;
			}
		}
		if (!IS_BOOL) {
			blob_offset += sizeof(T);
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
//! * @param value_ids_selvec The selection vector from i (< count) to the index in the children.value_ids selvec, to populate the parent's children
template <bool WRITE_DATA>
static bool ConvertToVariant(Vector &source, Vector &result, DataChunk &offsets, idx_t count, SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary, SelectionVector *value_ids_selvec) {
	auto &type = source.GetType();

	if (WRITE_DATA) {
		Printer::PrintF("Type: %s | Count: %d", type.ToString(), count);
	}

	auto physical_type = type.InternalType();
	auto logical_type = type.id();
	auto blob_offset_data = OffsetData::GetBlob(offsets);
	auto values_offset_data = OffsetData::GetValues(offsets);

	//! value
	auto &blob = VariantVector::GetValue(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);

	//! values
	auto &values = VariantVector::GetValues(result);
	auto values_data = ListVector::GetData(values);

	//! children
	auto &children = VariantVector::GetChildren(result);
	auto children_data = ListVector::GetData(children);

	//! keys
	auto &keys = VariantVector::GetKeys(result);
	auto keys_data = ListVector::GetData(keys);

	//! type_ids
	auto &type_ids = VariantVector::GetValuesTypeId(result);
	auto type_ids_data = FlatVector::GetData<uint8_t>(type_ids);

	//! byte_offset
	auto &byte_offset = VariantVector::GetValuesByteOffset(result);
	auto byte_offset_data = FlatVector::GetData<uint32_t>(byte_offset);

	//! key_id
	auto &key_id = VariantVector::GetChildrenKeyId(result);
	auto key_id_data = FlatVector::GetData<uint32_t>(key_id);
	auto &key_id_validity = FlatVector::Validity(key_id);

	//! value_id
	auto &value_id = VariantVector::GetChildrenValueId(result);
	auto value_id_data = FlatVector::GetData<uint32_t>(value_id);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	if (type.IsNested()) {
		auto keys_offset_data = OffsetData::GetKeys(offsets);
		auto children_offset_data = OffsetData::GetChildren(offsets);
		//! FIXME: use logical type instead
		if (physical_type == PhysicalType::LIST) {
			auto source_data = source_format.GetData<list_entry_t>(source_format);

			//! Create a selection vector that maps to the right row in the 'result' for the child vector
			auto list_size = ListVector::GetListSize(source);
			SelectionVector new_selection(0, list_size);
			SelectionVector children_selection(0, list_size);
			for (idx_t i = 0; i < count; i++) {
				const auto index = source_format.sel->get_index(i);
				const auto result_index = selvec ? selvec->get_index(i) : i;
				auto &entry = source_data[index];

				auto &blob_offset = blob_offset_data[result_index];
				auto &values_list_entry = values_data[result_index];
				auto &children_list_entry = children_data[result_index];

				//! values
				if (WRITE_DATA) {
					//! type_id + byte_offset
					auto values_offset = values_list_entry.offset + values_offset_data[result_index];
					type_ids_data[values_offset] = static_cast<uint8_t>(VariantLogicalType::ARRAY);
					byte_offset_data[values_offset] = blob_offset;
					if (value_ids_selvec) {
						//! Set for the parent where this child lives in the 'values' list
						value_id_data[value_ids_selvec->get_index(i)] = values_offset;
					}
				}
				values_offset_data[result_index]++;

				//! value
				const auto length_varint_size = GetVarintSize(entry.length);
				const auto child_offset_varint_size = GetVarintSize(children_offset_data[result_index]);
				if (WRITE_DATA) {
					auto &blob_value = blob_data[result_index];
					auto blob_value_data = data_ptr_cast(blob_value.GetDataWriteable());

					VarintEncode(entry.length, blob_value_data + blob_offset);
					VarintEncode(children_offset_data[result_index], blob_value_data + blob_offset + length_varint_size);
				}
				blob_offset += length_varint_size + child_offset_varint_size;

				auto children_offset = children_list_entry.offset + children_offset_data[result_index];
				for (idx_t child_idx = 0; child_idx < entry.length; child_idx++) {
					//! Set up the selection vector for the child of the list vector
					new_selection.set_index(child_idx + entry.offset, result_index);
					if (WRITE_DATA) {
						children_selection.set_index(child_idx + entry.offset, children_offset + child_idx);
						key_id_validity.SetInvalid(children_offset + child_idx);
					}
				}

				//! children
				children_offset_data[result_index] += entry.length;
			}
			//! Now write the child vector of the list (for all rows)
			auto &entry = ListVector::GetEntry(source);
			ConvertToVariant<WRITE_DATA>(entry, result, offsets, list_size, &new_selection, keys_selvec, dictionary, &children_selection);
		} else if (physical_type == PhysicalType::STRUCT) {
			auto &children =  StructVector::GetEntries(source);
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
				auto &values_list_entry = values_data[result_index];
				auto &children_list_entry = children_data[result_index];
				auto &keys_list_entry = keys_data[result_index];

				//! values
				if (WRITE_DATA) {
					//! type_id + byte_offset
					auto values_offset = values_list_entry.offset + values_offset_data[result_index];
					type_ids_data[values_offset] = static_cast<uint8_t>(VariantLogicalType::OBJECT);
					byte_offset_data[values_offset] = blob_offset;
					if (value_ids_selvec) {
						//! Set for the parent where this child lives in the 'values' list
						value_id_data[value_ids_selvec->get_index(i)] = values_offset;
					}
				}
				values_offset_data[result_index]++;

				//! value
				const auto length_varint_size = GetVarintSize(children.size());
				const auto child_offset_varint_size = GetVarintSize(children_offset_data[result_index]);
				if (WRITE_DATA) {
					auto &blob_value = blob_data[result_index];
					auto blob_value_data = data_ptr_cast(blob_value.GetDataWriteable());

					VarintEncode(children.size(), blob_value_data + blob_offset);
					VarintEncode(children_offset_data[result_index], blob_value_data + blob_offset + length_varint_size);
				}
				blob_offset += length_varint_size + child_offset_varint_size;

				//! children
				if (WRITE_DATA) {
					auto children_offset = children_list_entry.offset + children_offset_data[result_index];
					auto keys_offset = keys_list_entry.offset + keys_offset_data[result_index];
					for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
						key_id_data[children_offset + child_idx] = keys_offset_data[result_index] + child_idx;
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

				ConvertToVariant<WRITE_DATA>(child, result, offsets, count, selvec, keys_selvec, dictionary, &children_selection);
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
	} else if (physical_type == PhysicalType::VARCHAR) {
		auto source_data = source_format.GetData<string_t>(source_format);
		for (idx_t i = 0; i < count; i++) {
			auto index = source_format.sel->get_index(i);
			auto &val = source_data[index];

			auto result_index = selvec ? selvec->get_index(i) : i;
			auto &blob_offset = blob_offset_data[result_index];
			auto &values_list_entry = values_data[result_index];

			//! values
			if (WRITE_DATA) {
				//! type_id + byte_offset
				auto values_offset = values_list_entry.offset + values_offset_data[result_index];
				type_ids_data[values_offset] = static_cast<uint8_t>(VariantLogicalType::VARCHAR);
				byte_offset_data[values_offset] = blob_offset;
				if (value_ids_selvec) {
					//! Set for the parent where this child lives in the 'values' list
					value_id_data[value_ids_selvec->get_index(i)] = values_offset;
				}
			}

			//! value
			auto str_size = val.GetSize();
			auto str_length_varint_size = GetVarintSize(str_size);
			if (WRITE_DATA) {
				auto &blob_value = blob_data[result_index];
				auto blob_value_data = data_ptr_cast(blob_value.GetDataWriteable());

				VarintEncode(str_size, blob_value_data + blob_offset);
				memcpy(blob_value_data + blob_offset + str_length_varint_size, val.GetData(), str_size);
			}
			blob_offset += str_length_varint_size + str_size;

			values_offset_data[result_index]++;
		}
	} else {
		//! FIXME: use logical type instead
		switch (physical_type) {
			case PhysicalType::BOOL:
				return ConvertPrimitiveToVariant<WRITE_DATA, bool, true>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::INT8:
				return ConvertPrimitiveToVariant<WRITE_DATA, int8_t, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::INT16:
				return ConvertPrimitiveToVariant<WRITE_DATA, int16_t, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::INT32:
				return ConvertPrimitiveToVariant<WRITE_DATA, int32_t, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::INT64:
				return ConvertPrimitiveToVariant<WRITE_DATA, int64_t, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::UINT8:
				return ConvertPrimitiveToVariant<WRITE_DATA, uint8_t, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::UINT16:
				return ConvertPrimitiveToVariant<WRITE_DATA, uint16_t, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::UINT32:
				return ConvertPrimitiveToVariant<WRITE_DATA, uint32_t, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::UINT64:
				return ConvertPrimitiveToVariant<WRITE_DATA, uint64_t, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::INT128:
				return ConvertPrimitiveToVariant<WRITE_DATA, hugeint_t, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::UINT128:
				return ConvertPrimitiveToVariant<WRITE_DATA, uhugeint_t, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::FLOAT:
				return ConvertPrimitiveToVariant<WRITE_DATA, float, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::DOUBLE:
				return ConvertPrimitiveToVariant<WRITE_DATA, double, false>(source, result, offsets, count, selvec, value_ids_selvec);
			case PhysicalType::INTERVAL:
				return ConvertPrimitiveToVariant<WRITE_DATA, interval_t, false>(source, result, offsets, count, selvec, value_ids_selvec);
			default:
				throw InternalException("Invalid PhysicalType for ConvertToVariant");
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
	offsets.Initialize(Allocator::DefaultAllocator(), {
		LogicalType::UINTEGER,
		LogicalType::UINTEGER,
		LogicalType::UINTEGER,
		LogicalType::UINTEGER
	}, count);
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

	//! First pass - collect sizes/offsets
	InitializeOffsets(offsets, count);
	ConvertToVariant<false>(source, result, offsets, count, nullptr, keys_selvec, dictionary, nullptr);

	idx_t keys_selvec_size;
	InitializeVariants(offsets, result, keys_selvec, keys_selvec_size);

	//! Second pass - actually construct the variants
	InitializeOffsets(offsets, count);
	ConvertToVariant<true>(source, result, offsets, count, nullptr, keys_selvec, dictionary, nullptr);

	keys_entry.Slice(keys_selvec, keys_selvec_size);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

} // namespace duckdb
