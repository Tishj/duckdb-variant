#define DUCKDB_EXTENSION_MAIN

#include "variant_extension.hpp"
#include "variant_functions.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
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

static void ToVariantFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	CastParameters parameters;
	VariantFunctions::CastToVARIANT(input, result, args.size(), parameters);
}

static unique_ptr<FunctionData> ToVariantBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	bound_function.return_type = CreateVariantType();
	return nullptr;
}

static void LoadInternal(DatabaseInstance &instance) {
	// add the "variant" type
	auto variant_type = CreateVariantType();
	ExtensionUtil::RegisterType(instance, VARIANT_TYPE_NAME, variant_type);

	// add the casts to and from VARIANT type
	ExtensionUtil::RegisterCastFunction(instance, LogicalType::JSON(), variant_type,
	                                    VariantFunctions::CastJSONToVARIANT);
	ExtensionUtil::RegisterCastFunction(instance, variant_type, LogicalType::JSON(),
	                                    VariantFunctions::CastVARIANTToJSON);
	ScalarFunction to_variant_func("to_variant", {LogicalType::ANY}, variant_type, ToVariantFunction);
	to_variant_func.bind = ToVariantBind;
	ExtensionUtil::RegisterFunction(instance, to_variant_func);
}

void VariantExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string VariantExtension::Name() {
	return "variant";
}

std::string VariantExtension::Version() const {
#ifdef EXT_VERSION_VARIANT
	return EXT_VERSION_VARIANT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void variant_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::VariantExtension>();
}

DUCKDB_EXTENSION_API const char *variant_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
