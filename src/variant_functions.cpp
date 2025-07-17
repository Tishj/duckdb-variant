#include "variant_extension.hpp"
#include "variant_functions.hpp"
#include "yyjson.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/selection_vector.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

namespace {

//! ------------ JSON -> Variant ------------

struct VariantConversionState {
public:
	explicit VariantConversionState() {
	}

public:
	//! Reset for every row
	void Reset() {
		stream.Rewind();

		//! Update the total count
		value_count += row_value_count;
		children_count += row_children_count;
		keys_count += row_keys_count;
		key_ids_count += row_key_ids_count;

		//! Reset the row-local counts
		row_value_count = 0;
		row_children_count = 0;
		row_keys_count = 0;
		row_key_ids_count = 0;
	}
public:
	uint32_t AddString(Vector &vec, string_t str) {
		auto it = dictionary.find(str);

		uint32_t dict_index;
		if (it == dictionary.end()) {
			auto vec_data = FlatVector::GetData<string_t>(vec);
			auto dict_count = dictionary.size();
			if (dict_count >= dictionary_capacity) {
				auto new_capacity = NextPowerOfTwo(dictionary_capacity + 1);
				vec.Resize(dictionary_capacity, new_capacity);
				dictionary_capacity = new_capacity;
			}
			vec_data[dict_count] = StringVector::AddStringOrBlob(vec, std::move(str));
			it = dictionary.emplace(vec_data[dict_count], dict_count).first;
		}
		dict_index = it->second;
		auto keys_idx = keys_count + row_keys_count++;
		if (!sel_vec_capacity || keys_idx >= sel_vec_capacity) {
			//! Reinitialize the selection vector
			auto new_capacity = !sel_vec_capacity ? STANDARD_VECTOR_SIZE : NextPowerOfTwo(sel_vec_capacity + 1);
			auto new_selection_data = make_shared_ptr<SelectionData>(new_capacity);
			memcpy(new_selection_data->owned_data.get(), sel_vec.data(), sizeof(sel_t) * sel_vec_capacity);
			sel_vec.Initialize(std::move(new_selection_data));
			sel_vec_capacity = new_capacity;
		}
		sel_vec.set_index(keys_idx, dict_index);
		return keys_idx;
	}
public:
	idx_t children_count = 0;
	idx_t key_ids_count = 0;
	idx_t keys_count = 0;
	idx_t key_id = 0;
	idx_t value_count = 0;
	//! Record the relationship between index in the 'keys' (child) and the index in the dictionary
	SelectionVector sel_vec;
	idx_t sel_vec_capacity = 0;
	//! Unsure uniqueness of the dictionary entries
	string_map_t<idx_t> dictionary;
	idx_t dictionary_capacity = STANDARD_VECTOR_SIZE;
public:
	//! State for the current row

	//! amount of values in the row
	idx_t row_value_count = 0;
	//! amount of children in the row
	idx_t row_children_count = 0;
	//! amount of keys in the row (unique)
	idx_t row_keys_count = 0;
	//! amount of key_ids in the row
	idx_t row_key_ids_count = 0;
	//! stream used to write the binary data
	MemoryStream stream;
};

} // namespace

static optional_idx ConvertJSON(yyjson_val *val, Vector &result, VariantConversionState &state);

static bool ConvertJSONArray(yyjson_val *arr, Vector &result, VariantConversionState &state) {
	auto &children = VariantVector::GetChildren(result);
	auto &children_entry = ListVector::GetEntry(children);
	auto children_entry_data = FlatVector::GetData<uint32_t>(children_entry);

	yyjson_arr_iter iter;
	yyjson_arr_iter_init(arr, &iter);

	//! Write the 'value' blob for the ARRAY
	uint32_t count = iter.max;
	auto start_child_index = state.children_count + state.row_children_count;
	state.stream.WriteData(const_data_ptr_cast(&count), sizeof(uint32_t));
	state.stream.WriteData(const_data_ptr_cast(&start_child_index), sizeof(uint32_t));

	//! Reserve these indices for the array
	state.row_children_count += count;

	auto &values = VariantVector::GetValues(result);
	ListVector::Reserve(values, state.value_count + state.row_value_count + count);
	ListVector::Reserve(children, state.children_count + state.row_children_count);

	//! Iterate over all the children in the Array
	while (yyjson_arr_iter_has_next(&iter)) {
		auto val = yyjson_arr_iter_next(&iter);

		auto child_index = ConvertJSON(val, result, state);
		if (!child_index.IsValid()) {
			return false;
		}

		//! Set the child index
		children_entry_data[start_child_index++] = child_index.GetIndex();
	}
	return true;
}

static bool ConvertJSONObject(yyjson_val *obj, Vector &result, VariantConversionState &state) {
	auto &keys = VariantVector::GetKeys(result);
	auto &keys_entry = ListVector::GetEntry(keys);

	auto &key_ids = VariantVector::GetKeyIds(result);
	auto &key_ids_entry = ListVector::GetEntry(key_ids);
	auto key_ids_entry_data = FlatVector::GetData<uint32_t>(key_ids_entry);

	auto &children = VariantVector::GetChildren(result);
	auto &children_entry = ListVector::GetEntry(children);
	auto children_entry_data = FlatVector::GetData<uint32_t>(children_entry);

	yyjson_obj_iter iter;
	yyjson_obj_iter_init(obj, &iter);

	//! Write the 'value' blob for the OBJECT
	uint32_t count = iter.max;
	auto start_keys_index = state.key_ids_count + state.row_key_ids_count;
	auto start_child_index = state.children_count + state.row_children_count;
	state.stream.WriteData(const_data_ptr_cast(&count), sizeof(uint32_t));
	state.stream.WriteData(const_data_ptr_cast(&start_keys_index), sizeof(uint32_t));
	state.stream.WriteData(const_data_ptr_cast(&start_child_index), sizeof(uint32_t));

	//! Reserve these indices for the object
	state.row_key_ids_count += count;
	state.row_children_count += count;

	auto &values = VariantVector::GetValues(result);
	ListVector::Reserve(values, state.value_count + state.row_value_count + count);
	ListVector::Reserve(key_ids, state.key_ids_count + state.row_key_ids_count);
	ListVector::Reserve(children, state.children_count + state.row_children_count);

	//! Iterate over all the children in the Object
	yyjson_val *key, *val;
	while ((key = yyjson_obj_iter_next(&iter))) {
		auto key_string = yyjson_get_str(key);
		uint32_t key_string_len = unsafe_yyjson_get_len(key);

		auto str = string_t(key_string, key_string_len);
		auto keys_index = state.AddString(keys_entry, str);

		//! Set the key_id
		key_ids_entry_data[start_keys_index++] = keys_index;

		val = yyjson_obj_iter_get_val(key);
		auto child_index = ConvertJSON(val, result, state);
		if (!child_index.IsValid()) {
			return false;
		}
		//! Set the child index
		children_entry_data[start_child_index++] = child_index.GetIndex();
	}
	return true;
}

static optional_idx ConvertJSON(yyjson_val *val, Vector &result, VariantConversionState &state) {
	auto &type_ids = VariantVector::GetValuesTypeId(result);
	auto &byte_offsets = VariantVector::GetValuesByteOffset(result);

	auto type_ids_data = FlatVector::GetData<uint8_t>(type_ids);
	auto byte_offsets_data = FlatVector::GetData<uint32_t>(byte_offsets);

	auto index = state.value_count + state.row_value_count++;
	byte_offsets_data[index] = state.stream.GetPosition();

	if (unsafe_yyjson_is_null(val)) {
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL);
		return index;
	}

	switch (unsafe_yyjson_get_tag(val)) {
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_RAW | YYJSON_SUBTYPE_NONE: {
		auto str = unsafe_yyjson_get_str(val);
		uint32_t length = unsafe_yyjson_get_len(val);
		state.stream.WriteData(const_data_ptr_cast(&length), sizeof(uint32_t));
		state.stream.WriteData(const_data_ptr_cast(str), length);
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::VARCHAR);
		break;
	}
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE: {
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::ARRAY);
		if (!ConvertJSONArray(val, result, state)) {
			return optional_idx();
		}
		break;
	}
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE: {
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::OBJECT);
		if (!ConvertJSONObject(val, result, state)) {
			return optional_idx();
		}
		break;
	}
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE: {
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::BOOL_TRUE);
		break;
	}
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE: {
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::BOOL_FALSE);
		break;
	}
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT: {
		auto value = unsafe_yyjson_get_uint(val);
		state.stream.WriteData(const_data_ptr_cast(&value), sizeof(uint64_t));
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::UINT64);
		break;
	}
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT: {
		auto value = unsafe_yyjson_get_sint(val);
		state.stream.WriteData(const_data_ptr_cast(&value), sizeof(int64_t));
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::INT64);
		break;
	}
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL: {
		auto value = unsafe_yyjson_get_real(val);
		state.stream.WriteData(const_data_ptr_cast(&value), sizeof(double));
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::DOUBLE);
		break;
	}
	default:
		throw InternalException("Unknown yyjson tag in ConvertJSON");
	}
	return index;
}

bool VariantFunctions::CastJSONToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	auto source_data = source_format.GetData<string_t>(source_format);
	VariantConversionState state;

	auto &keys = VariantVector::GetKeys(result);
	auto keys_data = FlatVector::GetData<list_entry_t>(keys);
	ListVector::SetListSize(keys, 0);

	auto &key_ids = VariantVector::GetKeyIds(result);
	auto key_ids_data = FlatVector::GetData<list_entry_t>(key_ids);
	ListVector::SetListSize(key_ids, 0);

	auto &children = VariantVector::GetChildren(result);
	auto children_data = FlatVector::GetData<list_entry_t>(children);
	ListVector::SetListSize(children, 0);

	auto &values = VariantVector::GetValues(result);
	auto values_data = FlatVector::GetData<list_entry_t>(values);
	ListVector::SetListSize(values, 0);

	auto &value = VariantVector::GetValue(result);
	auto value_data = FlatVector::GetData<string_t>(value);

	ListVector::Reserve(values, state.value_count + state.row_value_count + count);
	for (idx_t i = 0; i < count; i++) {
		auto source_index = source_format.sel->get_index(i);
		auto &val = source_data[source_index];

		auto *doc = yyjson_read(val.GetData(), val.GetSize(), 0);
		auto *root = yyjson_doc_get_root(doc);

		//! keys
		auto &keys_list_entry = keys_data[i];
		keys_list_entry.offset = ListVector::GetListSize(keys);

		//! key_ids
		auto &key_ids_list_entry = key_ids_data[i];
		key_ids_list_entry.offset = ListVector::GetListSize(key_ids);

		//! children
		auto &children_list_entry = children_data[i];
		children_list_entry.offset = ListVector::GetListSize(children);

		//! values
		auto &values_list_entry = values_data[i];
		values_list_entry.offset = ListVector::GetListSize(values);

		auto value_index = ConvertJSON(root, result, state);
		if (!value_index.IsValid()) {
			return false;
		}

		//! keys
		keys_list_entry.length = state.row_keys_count;
		ListVector::SetListSize(keys, keys_list_entry.offset + keys_list_entry.length);

		//! key_ids
		key_ids_list_entry.length = state.row_key_ids_count;
		ListVector::SetListSize(key_ids, key_ids_list_entry.offset + key_ids_list_entry.length);

		//! children
		children_list_entry.length = state.row_children_count;
		ListVector::SetListSize(children, children_list_entry.offset + children_list_entry.length);

		//! values
		values_list_entry.length = state.row_value_count;
		ListVector::SetListSize(values, values_list_entry.offset + values_list_entry.length);

		//! value
		auto size = state.stream.GetPosition();
		auto stream_data = state.stream.GetData();
		value_data[i] = StringVector::AddStringOrBlob(value, reinterpret_cast<const char *>(stream_data), size);
		state.Reset();
	}

	auto &keys_entry = ListVector::GetEntry(keys);
	keys_entry.Slice(state.sel_vec, state.keys_count);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return true;
}

//! ------------ Variant -> JSON ------------

namespace {

struct UnifiedVariantVector {
	//! The 'keys' list (dictionary)
	static UnifiedVectorFormat &GetKeys(RecursiveUnifiedVectorFormat &vec) {
		return vec.children[0].unified;
	}
	//! The 'keys' list entry
	static UnifiedVectorFormat &GetKeysEntry(RecursiveUnifiedVectorFormat &vec) {
		return vec.children[0].children[0].unified;
	}
	//! The 'key_ids' list
	static UnifiedVectorFormat &GetKeyIds(RecursiveUnifiedVectorFormat &vec) {
		return vec.children[1].unified;
	}
	//! The 'key_ids' list entry
	static UnifiedVectorFormat &GetKeyIdsEntry(RecursiveUnifiedVectorFormat &vec) {
		return vec.children[1].children[0].unified;
	}
	//! The 'children' list
	static UnifiedVectorFormat &GetChildren(RecursiveUnifiedVectorFormat &vec) {
		return vec.children[2].unified;
	}
	//! The 'children' list entry
	static UnifiedVectorFormat &GetChildrenEntry(RecursiveUnifiedVectorFormat &vec) {
		return vec.children[2].children[0].unified;
	}
	//! The 'values' list
	static UnifiedVectorFormat &GetValues(RecursiveUnifiedVectorFormat &vec) {
		return vec.children[3].unified;
	}
	//! The 'type_id' inside the 'values' list
	static UnifiedVectorFormat &GetValuesTypeId(RecursiveUnifiedVectorFormat &vec) {
		auto &values = vec.children[3];
		return values.children[0].children[0].unified;
	}
	//! The 'byte_offset' inside the 'values' list
	static UnifiedVectorFormat &GetValuesByteOffset(RecursiveUnifiedVectorFormat &vec) {
		auto &values = vec.children[3];
		return values.children[0].children[1].unified;
	}
	//! The binary blob 'value' encoding the Variant for the row
	static UnifiedVectorFormat &GetValue(RecursiveUnifiedVectorFormat &vec) {
		return vec.children[4].unified;
	}
};

} // namespace

yyjson_mut_val *ConvertVariant(yyjson_mut_doc *doc, RecursiveUnifiedVectorFormat &source, idx_t row, idx_t values_idx) {
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
	auto &children_entry = UnifiedVariantVector::GetChildrenEntry(source);
	auto children_entry_data = children_entry.GetData<uint32_t>(children_entry);

	//! keys
	auto &keys = UnifiedVariantVector::GetKeys(source);
	auto keys_data = keys.GetData<list_entry_t>(keys);
	auto &keys_entry = UnifiedVariantVector::GetKeysEntry(source);
	auto keys_entry_data = keys_entry.GetData<string_t>(keys_entry);

	//! key_ids
	auto &key_ids = UnifiedVariantVector::GetKeyIds(source);
	auto key_ids_data = key_ids.GetData<list_entry_t>(key_ids);
	auto &key_ids_entry = UnifiedVariantVector::GetKeyIdsEntry(source);
	auto key_ids_entry_data = key_ids_entry.GetData<uint32_t>(key_ids_entry);

	//! list entries
	auto values_list_entry = values_data[values.sel->get_index(row)];
	auto children_list_entry = children_data[children.sel->get_index(row)];
	auto keys_list_entry = keys_data[keys.sel->get_index(row)];
	auto key_ids_list_entry = key_ids_data[key_ids.sel->get_index(row)];

	//! The 'values' data of the value we're currently converting
	values_idx += values_list_entry.offset;
	auto type_id = static_cast<VariantLogicalType>(type_ids_data[type_ids.sel->get_index(values_idx)]);
	auto byte_offset = byte_offsets_data[byte_offsets.sel->get_index(values_idx)];

	//! The blob data of the Variant, accessed by byte offset retrieved above ^
	auto &value = UnifiedVariantVector::GetValue(source);
	auto value_data = value.GetData<string_t>(value);
	auto &blob = value_data[row];
	auto blob_data = const_data_ptr_cast(blob.GetData());

	switch (type_id) {
		case VariantLogicalType::VARIANT_NULL:
			return yyjson_mut_null(doc);
		case VariantLogicalType::BOOL_TRUE:
			return yyjson_mut_true(doc);
		case VariantLogicalType::BOOL_FALSE:
			return yyjson_mut_false(doc);
		case VariantLogicalType::INT64: {
			auto val = Load<int64_t>(blob_data + byte_offset);
			return yyjson_mut_sint(doc, val);
		}
		case VariantLogicalType::UINT64: {
			auto val = Load<uint64_t>(blob_data + byte_offset);
			return yyjson_mut_uint(doc, val);
		}
		case VariantLogicalType::DOUBLE: {
			auto val = Load<double>(blob_data + byte_offset);
			return yyjson_mut_real(doc, val);
		}
		case VariantLogicalType::VARCHAR: {
			auto string_length = Load<uint32_t>(blob_data + byte_offset);
			auto string_data = reinterpret_cast<const char *>(blob_data + byte_offset + sizeof(uint32_t));
			return yyjson_mut_strncpy(doc, string_data, static_cast<size_t>(string_length));
		}
		case VariantLogicalType::ARRAY: {
			auto count = Load<uint32_t>(blob_data + byte_offset);
			auto arr = yyjson_mut_arr(doc);
			if (!count) {
				return arr;
			}
			auto child_index_start = Load<uint32_t>(blob_data + byte_offset + sizeof(uint32_t));
			for (idx_t i = 0; i < count; i++) {
				auto index = children_entry.sel->get_index(children_list_entry.offset + child_index_start + i);
				auto child_index = children_entry_data[index];
				auto val = ConvertVariant(doc, source, row, child_index);
				if (!val) {
					return nullptr;
				}
				yyjson_mut_arr_add_val(arr, val);
			}
			return arr;
		}
		case VariantLogicalType::OBJECT: {
			auto count = Load<uint32_t>(blob_data + byte_offset);
			auto obj = yyjson_mut_obj(doc);
			if (!count) {
				return obj;
			}
			auto keys_index_start = Load<uint32_t>(blob_data + byte_offset + sizeof(uint32_t));
			auto child_index_start = Load<uint32_t>(blob_data + byte_offset + sizeof(uint32_t) + sizeof(uint32_t));

			for (idx_t i = 0; i < count; i++) {
				auto children_index = children_entry.sel->get_index(children_list_entry.offset + child_index_start + i);
				auto child_value_idx = children_entry_data[children_index];
				auto val = ConvertVariant(doc, source, row, child_value_idx);
				if (!val) {
					return nullptr;
				}
				auto key_ids_index = key_ids_entry.sel->get_index(key_ids_list_entry.offset + keys_index_start + i);
				auto child_key_id = key_ids_entry_data[key_ids_index];
				auto &key = keys_entry_data[keys_entry.sel->get_index(keys_list_entry.offset + child_key_id)];
				yyjson_mut_obj_put(obj, yyjson_mut_strncpy(doc, key.GetData(), key.GetSize()), val);
			}
			return obj;
		}
		default:
			throw InternalException("VariantLogicalType(%d) not handled", static_cast<uint8_t>(type_id));
	}

	return nullptr;
}

bool VariantFunctions::CastVARIANTToJSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(source, count, source_format);

	auto result_data = FlatVector::GetData<string_t>(result);
	auto doc = yyjson_mut_doc_new(nullptr);
	for (idx_t i = 0; i < count; i++) {
		
		auto json_val = ConvertVariant(doc, source_format, i, 0);
		if (!json_val) {
			return false;
		}

		//! TODO: make this safe (add a destructor to hold this heap-allocated memory)
		size_t len;
		auto json_data =
		    yyjson_mut_val_write_opts(json_val, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, &len, nullptr);
		if (!json_data) {
			throw InvalidInputException("Could not serialize the JSON to string, yyjson failed");
		}
		string_t res(json_data, static_cast<idx_t>(len));
		result_data[i] = StringVector::AddString(result, res);
		free(json_data);
	}
	yyjson_mut_doc_free(doc);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return true;
}

} // namespace duckdb
