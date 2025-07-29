#pragma once

#include "variant_functions.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct VariantUtils {
	static bool FindChildValues(RecursiveUnifiedVectorFormat &source, const PathComponent &component, optional_idx row,
	                            uint32_t *res, VariantNestedData *nested_data, idx_t count);
};

} // namespace duckdb
