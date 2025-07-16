#include "variant_extension.hpp"
#include "variant_functions.hpp"
#include "yyjson.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/typedefs.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

namespace {

// struct StringDictionary {
// public:
//	explicit StringDictionary(Vector &vec) : vec(vec) {}
// public:
//	uint32_t AddString(string_t str) {
//		auto it = indices.find(str);
//		if (it != indices.end()) {
//			return it.second;
//		}
//		auto dictionary_entry = StringVector::AddString(vec, std::move(str));
//		it = indices.emplace(std::move(dictionary_entry), indices.size());
//		return it.second;
//	}
// public:
//	Vector &vec;
//	string_map_t<uint32_t> indices;
//};

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
	idx_t children_count = 0;
	idx_t key_ids_count = 0;
	idx_t keys_count = 0;
	idx_t key_id = 0;
	idx_t value_count = 0;
	////! dictionary to store the 'keys' of the variant
	// StringDictionary dictionary;
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

struct VariantVector {
	//! The 'keys' list (dictionary)
	static Vector &GetKeys(Vector &vec) {
		return *StructVector::GetEntries(vec)[0];
	}
	//! The 'key_ids' list
	static Vector &GetKeyIds(Vector &vec) {
		return *StructVector::GetEntries(vec)[1];
	}
	//! The 'children' list
	static Vector &GetChildren(Vector &vec) {
		return *StructVector::GetEntries(vec)[2];
	}
	//! The 'values' list
	static Vector &GetValues(Vector &vec) {
		return *StructVector::GetEntries(vec)[3];
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
		return *StructVector::GetEntries(vec)[4];
	}
};

static bool ConvertJSON(yyjson_val *val, Vector &result, VariantConversionState &state);

static bool ConvertJSONObject(yyjson_val *obj, Vector &result, VariantConversionState &state) {
	auto &keys = VariantVector::GetKeys(result);
	auto &keys_entry = ListVector::GetEntry(keys);
	auto keys_entry_data = FlatVector::GetData<string_t>(keys_entry);

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

	//! Iterate over all the children in the Object
	yyjson_val *key, *val;
	while ((key = yyjson_obj_iter_next(&iter))) {
		auto key_string = yyjson_get_str(key);
		val = yyjson_obj_iter_get_val(key);

		//! Add the string to the dictionary (TODO: deduplicate these, probably with a string_map_t)
		auto keys_index = state.keys_count + state.row_keys_count++;
		keys_entry_data[keys_index] = StringVector::AddString(keys, string_t(key_string));

		//! Set the key_id
		auto key_ids_index = state.key_ids_count + state.row_key_ids_count++;
		key_ids_entry_data[key_ids_index] = keys_index;

		//! Set the child index
		auto children_index = state.children_count + state.row_children_count++;
		children_entry_data[children_index] = keys_index;

		if (!ConvertJSON(val, result, state)) {
			return false;
		}
	}
	return true;
}

static bool ConvertJSON(yyjson_val *val, Vector &result, VariantConversionState &state) {
	auto &type_ids = VariantVector::GetValuesTypeId(result);
	auto &byte_offsets = VariantVector::GetValuesByteOffset(result);

	auto type_ids_data = FlatVector::GetData<uint8_t>(type_ids);
	auto byte_offsets_data = FlatVector::GetData<uint32_t>(byte_offsets);

	auto index = state.value_count + state.row_value_count++;
	byte_offsets_data[index] = state.stream.GetPosition();

	if (unsafe_yyjson_is_null(val)) {
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL);
		return true;
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
		break;
	}
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE: {
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
	return true;
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

		ConvertJSON(root, result, state);

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
		value_data[i] = StringVector::AddString(value, string_t(reinterpret_cast<const char *>(value_data), size));
		state.Reset();
	}
	return true;
}

bool VariantFunctions::CastVARIANTToJSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	throw NotImplementedException("CastVARIANTToJSON");
}

} // namespace duckdb
