#include "variant_functions.hpp"
#include "variant_extension.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/string_map_set.hpp"

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

template <bool WRITE_DATA, class T>
static bool ConvertPrimitiveToVariant(Vector &source, Vector &result, DataChunk &offsets, idx_t count, SelectionVector *selvec) {
	const auto &type = source.GetType();

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
	auto byte_offset_data = FlatVector::GetData<uint8_t>(byte_offset);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	auto source_data = source_format.GetData<T>(source_format);
	for (idx_t i = 0; i < count; i++) {
		auto index = source_format.sel->get_index(i);

		auto result_index = selvec ? selvec->get_index(i) : i;
		auto &blob_offset = blob_offset_data[result_index];
		auto &values_list_entry = values_data[result_index];

		//! value
		if (WRITE_DATA) {
			auto &blob_value = blob_data[result_index];
			auto blob_value_data = data_ptr_cast(blob_value.GetDataWriteable());
			Store(source_data[index], blob_value_data + blob_offset);
		}
		blob_offset += sizeof(T);

		//! values
		if (WRITE_DATA) {
			//! type_id + byte_offset
			auto values_offset = values_list_entry.offset + values_offset_data[result_index];
			type_ids_data[values_offset] = static_cast<uint8_t>(type.id());
			byte_offset_data[values_offset] = blob_offset;
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
template <bool WRITE_DATA>
static bool ConvertToVariant(Vector &source, Vector &result, DataChunk &offsets, idx_t count, SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary) {
	auto &type = source.GetType();
	auto physical_type = type.InternalType();
	auto blob_offset_data = OffsetData::GetBlob(offsets);
	auto values_offset_data = OffsetData::GetChildren(offsets);

	//! value
	auto &blob = VariantVector::GetValue(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);

	//! values
	auto &values = VariantVector::GetValues(result);
	auto values_data = ListVector::GetData(values);

	//! children
	auto &children = VariantVector::GetChildren(result);
	auto children_data = ListVector::GetData(children);

	////! keys
	//auto &keys = VariantVector::GetKeys(result);
	//auto keys_data = ListVector::GetData(keys);

	////! keys_entry
	//auto &keys_entry = ListVector::GetEntry(keys);

	//! type_ids
	auto &type_ids = VariantVector::GetValuesTypeId(result);
	auto type_ids_data = FlatVector::GetData<uint8_t>(type_ids);

	//! byte_offset
	auto &byte_offset = VariantVector::GetValuesByteOffset(result);
	auto byte_offset_data = FlatVector::GetData<uint8_t>(byte_offset);

	//! key_id
	auto &key_id = VariantVector::GetChildrenKeyId(result);
	auto key_id_data = FlatVector::GetData<uint32_t>(key_id);

	//! value_id
	auto &value_id = VariantVector::GetChildrenValueId(result);
	auto value_id_data = FlatVector::GetData<uint32_t>(value_id);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	if (type.IsNested()) {
		auto keys_offset_data = OffsetData::GetKeys(offsets);
		auto children_offset_data = OffsetData::GetChildren(offsets);
		if (physical_type == PhysicalType::LIST) {
			auto source_data = source_format.GetData<list_entry_t>(source_format);

			//! Keep track of the indices where children of the list's values are stored 
			Vector child_offset(LogicalType::UINTEGER, offsets.size());
			auto child_offset_data = FlatVector::GetData<uint32_t>(child_offset);

			if (WRITE_DATA) {
				//! Initialize the child offsets to the current values offsets
				for (idx_t i = 0; i < offsets.size(); i++) {
					child_offset_data[i] = values_offset_data[i];
				}
				//! Offset this by the lists of the row, the values will be written after them
				//! For example: [[1,2,3],[4,5,6],[7,8]]
				//! Is stored in the 'values' list as: LLLEEEEEEEE
				//! Where L represents a LIST, and E represents an element
				for (idx_t i = 0; i < count; i++) {
					child_offset_data[selvec ? selvec->get_index(i) : i]++;
				}
			}

			//! Create a selection vector that maps to the right row for the child vector
			auto list_size = ListVector::GetListSize(source);
			SelectionVector new_selection(0, list_size);
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
					type_ids_data[values_offset] = static_cast<uint8_t>(type.id());
					byte_offset_data[values_offset] = blob_offset;
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

					// Write the value indices for the children
					if (WRITE_DATA) {
						//! value_id
						value_id_data[children_offset + child_idx] = child_offset_data[result_index] + child_idx;
					}
				}

				//! children
				children_offset_data[result_index] += entry.length;
				if (WRITE_DATA) {
					child_offset_data[result_index] += entry.length;
				}
			}
			//! Now write the child vector of the list (for all rows)
			auto &entry = ListVector::GetEntry(source);
			ConvertToVariant<WRITE_DATA>(entry, result, offsets, list_size, &new_selection, keys_selvec, dictionary);
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

			//! Keep track of the indices where children of the list's values are stored 
			Vector child_offset(LogicalType::UINTEGER, offsets.size());
			auto child_offset_data = FlatVector::GetData<uint32_t>(child_offset);

			if (WRITE_DATA) {
				//! Initialize the child offsets to the current values offsets
				for (idx_t i = 0; i < offsets.size(); i++) {
					child_offset_data[i] = values_offset_data[i];
				}
				for (idx_t i = 0; i < count; i++) {
					child_offset_data[selvec ? selvec->get_index(i) : i]++;
				}
			}

			for (idx_t i = 0; i < count; i++) {
				auto result_index = selvec ? selvec->get_index(i) : i;

				auto &blob_offset = blob_offset_data[result_index];
				auto &values_list_entry = values_data[result_index];
				auto &children_list_entry = children_data[result_index];
				//auto &keys_list_entry = keys_data[result_index];

				//! values
				if (WRITE_DATA) {
					//! type_id + byte_offset
					auto values_offset = values_list_entry.offset + values_offset_data[result_index];
					type_ids_data[values_offset] = static_cast<uint8_t>(type.id());
					byte_offset_data[values_offset] = blob_offset;
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
					for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
						value_id_data[children_offset + child_idx] = child_offset_data[result_index] + child_idx;
						key_id_data[children_offset + child_idx] = keys_offset_data[result_index] + child_idx;
						keys_selvec.set_index(keys_offset_data[result_index] + child_idx, dictionary_indices[child_idx]);
					}
				}
				children_offset_data[result_index] += children.size();
				keys_offset_data[result_index] += children.size();

				if (WRITE_DATA) {
					child_offset_data[result_index] += children.size();
				}
			}

			for (auto &child_ptr : children) {
				auto &child = *child_ptr;
				ConvertToVariant<WRITE_DATA>(child, result, offsets, count, selvec, keys_selvec, dictionary);
			}
		} else {
			throw NotImplementedException("Can't convert nested physical type '%s'", EnumUtil::ToString(physical_type));
		}
	} else if (physical_type == PhysicalType::VARCHAR) {
		//! TODO: Implement WRITE_DATA for strings
		auto source_data = source_format.GetData<string_t>(source_format);
		for (idx_t i = 0; i < count; i++) {
			auto index = source_format.sel->get_index(i);
			auto &val = source_data[index];

			auto result_index = selvec ? selvec->get_index(i) : i;
			auto &blob_offset = blob_offset_data[result_index];
			auto &values_list_entry = values_data[result_index];

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

			//! values
			if (WRITE_DATA) {
				//! type_id + byte_offset
				auto values_offset = values_list_entry.offset + values_offset_data[result_index];
				type_ids_data[values_offset] = static_cast<uint8_t>(type.id());
				byte_offset_data[values_offset] = blob_offset;
			}
			values_offset_data[result_index]++;
		}
	} else {
		switch (physical_type) {
			case PhysicalType::BOOL:
				return ConvertPrimitiveToVariant<WRITE_DATA, bool>(source, result, offsets, count, selvec);
			case PhysicalType::INT8:
				return ConvertPrimitiveToVariant<WRITE_DATA, int8_t>(source, result, offsets, count, selvec);
			case PhysicalType::INT16:
				return ConvertPrimitiveToVariant<WRITE_DATA, int16_t>(source, result, offsets, count, selvec);
			case PhysicalType::INT32:
				return ConvertPrimitiveToVariant<WRITE_DATA, int32_t>(source, result, offsets, count, selvec);
			case PhysicalType::INT64:
				return ConvertPrimitiveToVariant<WRITE_DATA, int64_t>(source, result, offsets, count, selvec);
			case PhysicalType::UINT8:
				return ConvertPrimitiveToVariant<WRITE_DATA, uint8_t>(source, result, offsets, count, selvec);
			case PhysicalType::UINT16:
				return ConvertPrimitiveToVariant<WRITE_DATA, uint16_t>(source, result, offsets, count, selvec);
			case PhysicalType::UINT32:
				return ConvertPrimitiveToVariant<WRITE_DATA, uint32_t>(source, result, offsets, count, selvec);
			case PhysicalType::UINT64:
				return ConvertPrimitiveToVariant<WRITE_DATA, uint64_t>(source, result, offsets, count, selvec);
			case PhysicalType::INT128:
				return ConvertPrimitiveToVariant<WRITE_DATA, hugeint_t>(source, result, offsets, count, selvec);
			case PhysicalType::UINT128:
				return ConvertPrimitiveToVariant<WRITE_DATA, uhugeint_t>(source, result, offsets, count, selvec);
			case PhysicalType::FLOAT:
				return ConvertPrimitiveToVariant<WRITE_DATA, float>(source, result, offsets, count, selvec);
			case PhysicalType::DOUBLE:
				return ConvertPrimitiveToVariant<WRITE_DATA, double>(source, result, offsets, count, selvec);
			case PhysicalType::VARCHAR:
				return ConvertPrimitiveToVariant<WRITE_DATA, string_t>(source, result, offsets, count, selvec);
			case PhysicalType::INTERVAL:
				return ConvertPrimitiveToVariant<WRITE_DATA, interval_t>(source, result, offsets, count, selvec);
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

static void InitializeVariants(DataChunk &offsets, Vector &result, SelectionVector &keys_selvec) {
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
}

bool VariantFunctions::CastToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	DataChunk offsets;
	offsets.Initialize(Allocator::DefaultAllocator(), {
		LogicalType::UINTEGER,
		LogicalType::UINTEGER,
		LogicalType::UINTEGER,
		LogicalType::UINTEGER
	}, count);

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
	ConvertToVariant<false>(source, result, offsets, count, nullptr, keys_selvec, dictionary);

	InitializeVariants(offsets, result, keys_selvec);

	//! Second pass - actually construct the variants
	InitializeOffsets(offsets, count);
	return ConvertToVariant<true>(source, result, offsets, count, nullptr, keys_selvec, dictionary);
}

} // namespace duckdb
