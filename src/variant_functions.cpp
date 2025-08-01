#include "variant_extension.hpp"
#include "variant_functions.hpp"
#include "yyjson.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

namespace {

//! ------------ JSON -> Variant ------------

template <class T>
static void VarintEncode(T val, MemoryStream &ser) {
	do {
		uint8_t byte = val & 127;
		val >>= 7;
		if (val != 0) {
			byte |= 128;
		}
		ser.WriteData(&byte, sizeof(uint8_t));
	} while (val != 0);
}

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
		keys_count += key_ids.size();

		//! Reset the row-local counts
		row_value_count = 0;
		row_children_count = 0;
		key_ids.clear();
	}

public:
	uint32_t AddString(Vector &vec, string_t str) {
		auto it = dictionary.find(str);

		uint32_t dict_index;
		if (it == dictionary.end()) {
			//! Key does not exist in the global dictionary yet
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

		auto local_it = key_ids.find(str);
		if (local_it == key_ids.end()) {
			auto vec_data = FlatVector::GetData<string_t>(vec);
			local_it = key_ids.emplace(vec_data[dict_index], key_ids.size()).first;
		}

		auto keys_idx = keys_count + local_it->second;
		if (!sel_vec_capacity || keys_idx >= sel_vec_capacity) {
			//! Reinitialize the selection vector
			auto new_capacity = !sel_vec_capacity ? STANDARD_VECTOR_SIZE : NextPowerOfTwo(sel_vec_capacity + 1);
			auto new_selection_data = make_shared_ptr<SelectionData>(new_capacity);
			memcpy(new_selection_data->owned_data.get(), sel_vec.data(), sizeof(sel_t) * sel_vec_capacity);
			sel_vec.Initialize(std::move(new_selection_data));
			sel_vec_capacity = new_capacity;
		}
		sel_vec.set_index(keys_idx, dict_index);
		return local_it->second;
	}

public:
	idx_t children_count = 0;
	idx_t keys_count = 0;
	idx_t key_id = 0;
	idx_t value_count = 0;
	//! Record the relationship between index in the 'keys' (child) and the index in the dictionary
	SelectionVector sel_vec;
	idx_t sel_vec_capacity = 0;
	//! Ensure uniqueness of the dictionary entries
	string_map_t<idx_t> dictionary;
	idx_t dictionary_capacity = STANDARD_VECTOR_SIZE;

public:
	//! State for the current row

	//! amount of values in the row
	idx_t row_value_count = 0;
	//! amount of children in the row
	idx_t row_children_count = 0;
	//! stream used to write the binary data
	MemoryStream stream;
	//! For the current row, the (duplicate eliminated) key ids
	string_map_t<idx_t> key_ids;
};

} // namespace

static optional_idx ConvertJSON(yyjson_val *val, Vector &result, VariantConversionState &state);

static bool ConvertJSONArray(yyjson_val *arr, Vector &result, VariantConversionState &state) {
	auto &children = VariantVector::GetChildren(result);
	auto &value_ids = VariantVector::GetChildrenValueId(result);
	auto &key_ids = VariantVector::GetChildrenKeyId(result);

	yyjson_arr_iter iter;
	yyjson_arr_iter_init(arr, &iter);

	//! Write the 'value' blob for the ARRAY
	uint32_t count = iter.max;
	auto start_child_index = state.children_count + state.row_children_count;
	VarintEncode(count, state.stream);
	VarintEncode(state.row_children_count, state.stream);

	//! Reserve these indices for the array
	state.row_children_count += count;

	auto &values = VariantVector::GetValues(result);
	ListVector::Reserve(values, state.value_count + state.row_value_count + count);
	ListVector::Reserve(children, state.children_count + state.row_children_count);

	auto value_ids_data = FlatVector::GetData<uint32_t>(value_ids);
	auto &key_ids_validity = FlatVector::Validity(key_ids);
	//! Iterate over all the children in the Array
	while (yyjson_arr_iter_has_next(&iter)) {
		auto val = yyjson_arr_iter_next(&iter);

		auto child_index = state.row_value_count;
		auto res = ConvertJSON(val, result, state);
		if (!res.IsValid()) {
			return false;
		}

		//! Set the child index
		key_ids_validity.SetInvalid(start_child_index);
		value_ids_data[start_child_index++] = child_index;
	}
	return true;
}

static bool ConvertJSONObject(yyjson_val *obj, Vector &result, VariantConversionState &state) {
	auto &keys = VariantVector::GetKeys(result);
	auto &keys_entry = ListVector::GetEntry(keys);

	auto &children = VariantVector::GetChildren(result);

	auto &key_ids = VariantVector::GetChildrenKeyId(result);
	auto &value_ids = VariantVector::GetChildrenValueId(result);

	yyjson_obj_iter iter;
	yyjson_obj_iter_init(obj, &iter);

	//! Write the 'value' blob for the OBJECT
	uint32_t count = iter.max;
	auto start_child_index = state.children_count + state.row_children_count;

	VarintEncode(count, state.stream);
	VarintEncode(state.row_children_count, state.stream);

	//! Reserve these indices for the object
	state.row_children_count += count;

	auto &values = VariantVector::GetValues(result);
	ListVector::Reserve(values, state.value_count + state.row_value_count + count);
	ListVector::Reserve(children, state.children_count + state.row_children_count);

	auto key_ids_data = FlatVector::GetData<uint32_t>(key_ids);
	auto value_ids_data = FlatVector::GetData<uint32_t>(value_ids);
	//! Iterate over all the children in the Object
	yyjson_val *key, *val;
	while ((key = yyjson_obj_iter_next(&iter))) {
		auto key_string = yyjson_get_str(key);
		uint32_t key_string_len = unsafe_yyjson_get_len(key);

		val = yyjson_obj_iter_get_val(key);
		auto child_index = state.row_value_count;
		auto res = ConvertJSON(val, result, state);
		if (!res.IsValid()) {
			return false;
		}

		auto str = string_t(key_string, key_string_len);
		auto key_id = state.AddString(keys_entry, str);

		//! Set the key_id
		key_ids_data[start_child_index] = key_id;
		//! Set the value_id
		value_ids_data[start_child_index] = child_index;
		start_child_index++;
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
		VarintEncode(length, state.stream);
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

	//! keys
	auto &keys = VariantVector::GetKeys(result);
	auto keys_data = FlatVector::GetData<list_entry_t>(keys);
	ListVector::SetListSize(keys, 0);

	//! children
	auto &children = VariantVector::GetChildren(result);
	auto children_data = FlatVector::GetData<list_entry_t>(children);
	ListVector::SetListSize(children, 0);

	//! values
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
		keys_list_entry.length = state.key_ids.size();
		ListVector::SetListSize(keys, keys_list_entry.offset + keys_list_entry.length);

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

	auto &dictionary = ListVector::GetEntry(keys);
	auto dictionary_size = state.dictionary.size();

	auto &sel = state.sel_vec;
	auto sel_size = state.keys_count;

	VariantVector::SortVariantKeys(dictionary, dictionary_size, sel, sel_size);
	dictionary.Slice(state.sel_vec, state.keys_count);
	dictionary.Flatten(state.keys_count);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return true;
}

//! ------------ Variant -> JSON ------------

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

	//! value_ids
	auto &value_ids = UnifiedVariantVector::GetChildrenValueId(source);
	auto value_ids_data = value_ids.GetData<uint32_t>(value_ids);

	//! key_ids
	auto &key_ids = UnifiedVariantVector::GetChildrenKeyId(source);
	auto key_ids_data = key_ids.GetData<uint32_t>(key_ids);

	//! keys
	auto &keys = UnifiedVariantVector::GetKeys(source);
	auto keys_data = keys.GetData<list_entry_t>(keys);
	auto &keys_entry = UnifiedVariantVector::GetKeysEntry(source);
	auto keys_entry_data = keys_entry.GetData<string_t>(keys_entry);

	//! list entries
	auto keys_list_entry = keys_data[keys.sel->get_index(row)];
	auto children_list_entry = children_data[children.sel->get_index(row)];
	auto values_list_entry = values_data[values.sel->get_index(row)];

	//! The 'values' data of the value we're currently converting
	values_idx += values_list_entry.offset;
	auto type_id = static_cast<VariantLogicalType>(type_ids_data[type_ids.sel->get_index(values_idx)]);
	auto byte_offset = byte_offsets_data[byte_offsets.sel->get_index(values_idx)];

	//! The blob data of the Variant, accessed by byte offset retrieved above ^
	auto &value = UnifiedVariantVector::GetValue(source);
	auto value_data = value.GetData<string_t>(value);
	auto &blob = value_data[row];
	auto blob_data = const_data_ptr_cast(blob.GetData());

	auto ptr = const_data_ptr_cast(blob_data + byte_offset);
	switch (type_id) {
	case VariantLogicalType::VARIANT_NULL:
		return yyjson_mut_null(doc);
	case VariantLogicalType::BOOL_TRUE:
		return yyjson_mut_true(doc);
	case VariantLogicalType::BOOL_FALSE:
		return yyjson_mut_false(doc);
	case VariantLogicalType::INT8: {
		auto val = Load<int8_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::INT16: {
		auto val = Load<int16_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::INT32: {
		auto val = Load<int32_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::INT64: {
		auto val = Load<int64_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::INT128: {
		auto val = Load<hugeint_t>(ptr);
		auto val_str = val.ToString();
		return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::UINT8: {
		auto val = Load<uint8_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::UINT16: {
		auto val = Load<uint16_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::UINT32: {
		auto val = Load<uint32_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::UINT64: {
		auto val = Load<uint64_t>(ptr);
		return yyjson_mut_uint(doc, val);
	}
	case VariantLogicalType::UINT128: {
		auto val = Load<uhugeint_t>(ptr);
		auto val_str = val.ToString();
		return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::UUID: {
		auto val = Value::UUID(Load<hugeint_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::INTERVAL: {
		auto val = Value::INTERVAL(Load<interval_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::FLOAT: {
		auto val = Load<float>(ptr);
		return yyjson_mut_real(doc, val);
	}
	case VariantLogicalType::DOUBLE: {
		auto val = Load<double>(ptr);
		return yyjson_mut_real(doc, val);
	}
	case VariantLogicalType::DATE: {
		auto val = Load<int32_t>(ptr);
		auto val_str = Date::ToString(date_t(val));
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::BLOB: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		auto val_str = Value::BLOB(const_data_ptr_cast(string_data), string_length).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::VARCHAR: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		return yyjson_mut_strncpy(doc, string_data, static_cast<size_t>(string_length));
	}
	case VariantLogicalType::DECIMAL: {
		auto width = VarintDecode<idx_t>(ptr);
		auto scale = VarintDecode<idx_t>(ptr);

		string val_str;
		if (width > DecimalWidth<int64_t>::max) {
			val_str = Decimal::ToString(Load<hugeint_t>(ptr), width, scale);
		} else if (width > DecimalWidth<int32_t>::max) {
			val_str = Decimal::ToString(Load<int64_t>(ptr), width, scale);
		} else if (width > DecimalWidth<int16_t>::max) {
			val_str = Decimal::ToString(Load<int32_t>(ptr), width, scale);
		} else {
			val_str = Decimal::ToString(Load<int16_t>(ptr), width, scale);
		}
		return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIME_MICROS: {
		auto val = Load<dtime_t>(ptr);
		auto val_str = Time::ToString(val);
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIME_MICROS_TZ: {
		auto val = Value::TIMETZ(Load<dtime_tz_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIMESTAMP_MICROS: {
		auto val = Load<timestamp_t>(ptr);
		auto val_str = Timestamp::ToString(val);
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIMESTAMP_SEC: {
		auto val = Value::TIMESTAMPSEC(Load<timestamp_sec_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIMESTAMP_NANOS: {
		auto val = Value::TIMESTAMPNS(Load<timestamp_ns_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIMESTAMP_MILIS: {
		auto val = Value::TIMESTAMPMS(Load<timestamp_ms_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIMESTAMP_MICROS_TZ: {
		auto val = Value::TIMESTAMPTZ(Load<timestamp_tz_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::ARRAY: {
		auto count = VarintDecode<uint32_t>(ptr);
		auto arr = yyjson_mut_arr(doc);
		if (!count) {
			return arr;
		}
		auto child_index_start = VarintDecode<uint32_t>(ptr);
		for (idx_t i = 0; i < count; i++) {
			auto index = value_ids.sel->get_index(children_list_entry.offset + child_index_start + i);
			auto child_index = value_ids_data[index];
			auto val = ConvertVariant(doc, source, row, child_index);
			if (!val) {
				return nullptr;
			}
			yyjson_mut_arr_add_val(arr, val);
		}
		return arr;
	}
	case VariantLogicalType::OBJECT: {
		auto count = VarintDecode<uint32_t>(ptr);
		auto obj = yyjson_mut_obj(doc);
		if (!count) {
			return obj;
		}
		auto child_index_start = VarintDecode<uint32_t>(ptr);

		for (idx_t i = 0; i < count; i++) {
			auto children_index = value_ids.sel->get_index(children_list_entry.offset + child_index_start + i);
			auto child_value_idx = value_ids_data[children_index];
			auto val = ConvertVariant(doc, source, row, child_value_idx);
			if (!val) {
				return nullptr;
			}
			auto key_ids_index = key_ids.sel->get_index(children_list_entry.offset + child_index_start + i);
			auto child_key_id = key_ids_data[key_ids_index];
			auto &key = keys_entry_data[keys_entry.sel->get_index(keys_list_entry.offset + child_key_id)];
			yyjson_mut_obj_put(obj, yyjson_mut_strncpy(doc, key.GetData(), key.GetSize()), val);
		}
		return obj;
	}
	case VariantLogicalType::BITSTRING: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		auto val_str = Value::BIT(const_data_ptr_cast(string_data), string_length).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::VARINT: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		auto val_str = Value::VARINT(const_data_ptr_cast(string_data), string_length).ToString();
		return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
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
		auto json_data = yyjson_mut_val_write_opts(json_val, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, &len, nullptr);
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
