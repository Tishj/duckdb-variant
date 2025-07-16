#include "variant_functions.hpp"

namespace duckdb {

bool VariantFunctions::CastJSONToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	throw NotImplementedException("CastJSONToVARIANT");
}

bool VariantFunctions::CastVARIANTToJSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	throw NotImplementedException("CastVARIANTToJSON");
}

} // namespace duckdb
