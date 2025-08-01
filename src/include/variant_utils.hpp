#pragma once

#include "variant_functions.hpp"
#include "variant_extension.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct VariantUtils {
	static bool FindChildValues(RecursiveUnifiedVectorFormat &source, const PathComponent &component, optional_idx row,
	                            uint32_t *res, VariantNestedData *nested_data, idx_t count);
	static bool CollectNestedData(RecursiveUnifiedVectorFormat &variant, VariantLogicalType expected_type,
	                              uint32_t *value_indices, idx_t count, optional_idx row, VariantNestedData *child_data,
	                              string &error);
};

} // namespace duckdb
