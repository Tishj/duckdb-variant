#include "variant_functions.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

static bool ConvertJSON(yyjson_doc *doc, yyjson_val *obj, Vector &result) {
	auto json_type = yyjson_get_type(obj);
	switch (json_type) {
		case YYJSON_TYPE_NONE: {

			break;
		}
		case YYJSON_TYPE_RAW: {

			break;
		}
		case YYJSON_TYPE_NULL: {

			break;
		}
		case YYJSON_TYPE_BOOL: {

			break;
		}
		case YYJSON_TYPE_NUM: {

			break;
		}
		case YYJSON_TYPE_STR: {

			break;
		}
		case YYJSON_TYPE_ARR: {

			break;
		}
		case YYJSON_TYPE_OBJ: {

			break;
		}
		default:
			throw InternalException("Unrecognized YYJSON_TYPE");
	}
	return true;
}

bool VariantFunctions::CastJSONToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	auto source_data = source_format.GetData<string_t>(source_format);

	for (idx_t i = 0; i < count; i++) {
		auto source_index = source_format.sel->get_index(i);
		auto &val = source_data[source_index];

		auto *doc = yyjson_read(val.GetData(), val.GetSize(), 0);
		auto *root = yyjson_doc_get_root(doc);

		ConvertJSON(doc, root, result);
	}
	return true;
}

bool VariantFunctions::CastVARIANTToJSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	throw NotImplementedException("CastVARIANTToJSON");
}

} // namespace duckdb
