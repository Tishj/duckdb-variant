#include "variant_extension.hpp"
#include "variant_functions.hpp"
#include "variant_utils.hpp"

#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

bool VariantFunctions::CastVARIANTToVARCHAR(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(source, count, source_format);

	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {

		auto val = VariantUtils::ConvertVariantToValue(source_format, i, 0);
		result_data[i] = StringVector::AddString(result, val.ToString());
	}

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return true;
}

} // namespace duckdb
