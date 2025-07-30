#pragma once

#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct VariantConversion {
	static Value ConvertVariantToValue(RecursiveUnifiedVectorFormat &source, idx_t row, idx_t values_idx);
};

struct VariantFunctions {
public:
	//! Generic VARIANT -> LogicalTypeId::ANY
	static bool CastFromVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	//! Generic LogicalTypeId::ANY -> VARIANT
	static bool CastToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	//! LogicalType::JSON -> VARIANT
	static bool CastJSONToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	//! VARIANT -> LogicalType::JSON
	static bool CastVARIANTToJSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	//! VARIANT -> LogicalType::VARCHAR
	static bool CastVARIANTToVARCHAR(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
};

enum class VariantChildLookupMode : uint8_t { BY_KEY, BY_INDEX };

struct PathComponent {
	VariantChildLookupMode lookup_mode;
	union {
		string_t key;
		uint32_t index;
	} payload;
};

//! Extract a Variant from a Variant
struct VariantExtract {
	struct BindData : public FunctionData {
	public:
		explicit BindData(const string &constant_path);

	public:
		unique_ptr<FunctionData> Copy() const override;
		bool Equals(const FunctionData &other) const override;

	public:
		string constant_path;
		//! NOTE: the keys in here reference data of the 'constant_path',
		//! the components can not be copied without reconstruction
		vector<PathComponent> components;
	};

	static void Func(DataChunk &input, ExpressionState &state, Vector &output);
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);
};

struct VariantNestedData {
	//! The amount of children in the nested structure
	uint32_t child_count;
	//! Index of the first child
	uint32_t children_idx;
};

struct VariantTypeof {
public:
	static void Func(DataChunk &input, ExpressionState &state, Vector &output);
};

} // namespace duckdb
