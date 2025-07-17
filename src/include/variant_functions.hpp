#pragma once

#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct VariantFunctions {
public:
	//! Generic LogicalTypeId::ANY -> VARIANT
	static bool CastToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	//! LogicalType::JSON -> VARIANT
	static bool CastJSONToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	//! VARIANT -> LogicalType::JSON
	static bool CastVARIANTToJSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
};

} // namespace duckdb
