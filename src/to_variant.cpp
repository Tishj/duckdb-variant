#include "variant_functions.hpp"
#include "variant_extension.hpp"

namespace duckdb {

bool VariantFunctions::CastToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	throw NotImplementedException("CastToVARIANT");
	return true;
}

} // namespace duckdb
