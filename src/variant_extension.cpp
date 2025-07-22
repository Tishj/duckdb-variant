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

static LogicalType CreateVariantType() {
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
	loader.RegisterCastFunction(LogicalType::JSON(), variant_type,
	                                    VariantFunctions::CastJSONToVARIANT);
	loader.RegisterCastFunction(variant_type, LogicalType::JSON(),
	                                    VariantFunctions::CastVARIANTToJSON);
	loader.RegisterCastFunction(LogicalType::ANY, variant_type, VariantFunctions::CastToVARIANT);
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
