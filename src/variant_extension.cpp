#define DUCKDB_EXTENSION_MAIN

#include "variant_extension.hpp"
#include "variant_functions.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

static constexpr auto VARIANT_TYPE_NAME = "VARIANT";

void VariantVector::SortVariantKeys(Vector &dictionary, idx_t dictionary_size, SelectionVector &sel, idx_t sel_size) {
	auto &allocator = Allocator::DefaultAllocator();
	auto dictionary_data = FlatVector::GetData<string_t>(dictionary);

	//! string + unsorted_index
	vector<std::pair<reference<string_t>, idx_t>> strings;
	strings.reserve(dictionary_size);
	for (idx_t i = 0; i < dictionary_size; i++) {
		strings.emplace_back(dictionary_data[i], i);
	}

	//! Sort the unique strings
	std::sort(strings.begin(), strings.end(),
	          [](const std::pair<reference<string_t>, idx_t> &a, const std::pair<reference<string_t>, idx_t> &b) {
		          return a.first.get() < b.first.get();
	          });

	bool is_already_sorted = true;
	vector<idx_t> unsorted_to_sorted(strings.size());
	for (idx_t i = 0; i < strings.size(); i++) {
		if (i != strings[i].second) {
			is_already_sorted = false;
		}
		unsorted_to_sorted[strings[i].second] = i;
	}

	if (is_already_sorted) {
		return;
	}

	//! Adjust the selection vector to point to the right dictionary index
	for (idx_t i = 0; i < sel_size; i++) {
		auto old_dictionary_index = sel.get_index(i);
		auto new_dictionary_index = unsorted_to_sorted[old_dictionary_index];
		sel.set_index(i, new_dictionary_index);
	}

	//! Finally, rewrite the dictionary itself
	auto copied_dictionary = allocator.Allocate(sizeof(string_t) * dictionary_size);
	auto copied_dictionary_data = reinterpret_cast<string_t *>(copied_dictionary.get());
	memcpy(copied_dictionary_data, dictionary_data, sizeof(string_t) * dictionary_size);

	for (idx_t i = 0; i < dictionary_size; i++) {
		dictionary_data[i] = copied_dictionary_data[strings[i].second];
	}
}

LogicalType CreateVariantType() {
	child_list_t<LogicalType> top_level_children;
	top_level_children.emplace_back("keys", LogicalType::LIST(LogicalType::VARCHAR));

	child_list_t<LogicalType> children_children;
	children_children.emplace_back("key_id", LogicalType::UINTEGER);
	children_children.emplace_back("value_id", LogicalType::UINTEGER);
	top_level_children.emplace_back("children", LogicalType::LIST(LogicalType::STRUCT(children_children)));

	child_list_t<LogicalType> values_children;
	values_children.emplace_back("type_id", LogicalTypeId::UTINYINT);
	values_children.emplace_back("byte_offset", LogicalTypeId::UINTEGER);
	top_level_children.emplace_back("values", LogicalType::LIST(LogicalType::STRUCT(values_children)));

	top_level_children.emplace_back("data", LogicalTypeId::BLOB);
	auto res = LogicalType::STRUCT(top_level_children);
	res.SetAlias(VARIANT_TYPE_NAME);
	return res;
}

static void LoadInternal(ExtensionLoader &loader) {
	// add the "variant" type
	auto variant_type = CreateVariantType();
	loader.RegisterType(VARIANT_TYPE_NAME, variant_type);

	// add the casts to and from VARIANT type
	loader.RegisterCastFunction(LogicalType::JSON(), variant_type, VariantFunctions::CastJSONToVARIANT, 5);
	loader.RegisterCastFunction(variant_type, LogicalType::JSON(), VariantFunctions::CastVARIANTToJSON, 5);
	loader.RegisterCastFunction(variant_type, LogicalType::VARCHAR, VariantFunctions::CastVARIANTToVARCHAR, 5);

	auto &casts = DBConfig::GetConfig(loader.GetDatabaseInstance()).GetCastFunctions();
	// Anything can be cast to VARIANT
	for (const auto &type : LogicalType::AllTypes()) {
		LogicalType source_type;

		switch (type.id()) {
		case LogicalTypeId::STRUCT:
			source_type = LogicalType::STRUCT({{"any", LogicalType::ANY}});
			break;
		case LogicalTypeId::LIST:
			source_type = LogicalType::LIST(LogicalType::ANY);
			break;
		case LogicalTypeId::MAP:
			source_type = LogicalType::MAP(LogicalType::ANY, LogicalType::ANY);
			break;
		case LogicalTypeId::UNION:
			source_type = LogicalType::UNION({{"any", LogicalType::ANY}});
			break;
		case LogicalTypeId::ARRAY:
			source_type = LogicalType::ARRAY(LogicalType::ANY, optional_idx());
			break;
		case LogicalTypeId::VARINT:
		case LogicalTypeId::BIT:
			//! TODO: we can't currently represent VARINT / BIT in a Variant
			continue;
		default:
			source_type = type;
		}
		casts.RegisterCastFunction(source_type, variant_type, VariantFunctions::CastToVARIANT, 5);
		if (type.id() != LogicalTypeId::VARCHAR) {
			casts.RegisterCastFunction(variant_type, source_type, VariantFunctions::CastFromVARIANT, 5);
		}
	}
}

void VariantExtension::Load(ExtensionLoader &db) {
	LoadInternal(db);
}
string VariantExtension::Name() {
	return "variant";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(variant, loader) {
	LoadInternal(loader);
}
}
