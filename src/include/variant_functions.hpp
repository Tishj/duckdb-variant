#pragma once

#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct VariantFunctions {
public:
	static bool CastJSONToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static bool CastVARIANTToJSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
};

} // namespace duckdb
