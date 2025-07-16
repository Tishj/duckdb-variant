#include "variant_functions.hpp"
#include "yyjson.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

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
		row_value_count = 0;
		row_children_count = 0;
		row_keys_count = 0;
		row_key_ids_count = 0;
	}
public:
	idx_t children_count = 0;
	idx_t key_ids_count = 0;
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

static bool ConvertJSONObject(yyjson_val *obj, Vector &result, VariantConversionState &state, idx_t row) {
	auto &keys = VariantVector::GetKeys(result);

	yyjson_obj_iter iter;
	yyjson_obj_iter_init(obj, &iter);
	yyjson_val *key, *val;
	while ((key = yyjson_obj_iter_next(&iter))) {
		auto key_string = yyjson_get_str(key);
		val = yyjson_obj_iter_get_val(key);

		auto byte_offset = state.stream.GetPosition();
		auto dictionary_string = StringVector::AddString(keys, string_t(key_string));
	}

	return true;
}

static bool ConvertJSON(yyjson_doc *doc, yyjson_val *obj, Vector &result, VariantConversionState &state, idx_t row) {
	auto json_type = yyjson_get_type(obj);
	switch (json_type) {
	case YYJSON_TYPE_RAW: {
		//! inf / nan ???

		break;
	}
	case YYJSON_TYPE_NULL: {

		break;
	}
	case YYJSON_TYPE_BOOL: {

		break;
	}
	case YYJSON_TYPE_NUM: {

		break;
	}
	case YYJSON_TYPE_STR: {

		break;
	}
	case YYJSON_TYPE_ARR: {

		break;
	}
	case YYJSON_TYPE_OBJ: {
		ConvertJSONObject(obj, result, state, row);
		break;
	}
	default:
		throw InternalException("Unrecognized YYJSON_TYPE");
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

		ConvertJSON(doc, root, result, state, i);

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
		state.Reset();
	}
	return true;
}

bool VariantFunctions::CastVARIANTToJSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	throw NotImplementedException("CastVARIANTToJSON");
}

} // namespace duckdb
