#include "variant_extension.hpp"
#include "variant_functions.hpp"
#include "variant_utils.hpp"
#include "duckdb/function/scalar/regexp.hpp"

namespace duckdb {

namespace {

using child_lookup_func_t =
    std::function<void(RecursiveUnifiedVectorFormat &source, const PathComponent &component, optional_idx row,
                       uint32_t *res, VariantNestedData *nested_data, idx_t count)>;

enum class PathParsingState : uint8_t { BASE, KEY, INDEX };

} // namespace

using regexp_util::TryParseConstantPattern;

vector<PathComponent> ParsePath(const string &path) {
	vector<PathComponent> components;
	auto state = PathParsingState::BASE;

	idx_t i = 0;
	while (i < path.size()) {
		switch (state) {
		case PathParsingState::BASE: {
			if (path[i] == '.') {
				// Skip dot, move to key state
				i++;
				state = PathParsingState::KEY;
			} else if (path[i] == '[') {
				// Start of an index
				i++;
				state = PathParsingState::INDEX;
			} else {
				// Start of key at base
				state = PathParsingState::KEY;
			}
			break;
		}
		case PathParsingState::KEY: {
			// Parse key until next '.' or '['
			idx_t start = i;
			while (i < path.size() && path[i] != '.' && path[i] != '[') {
				i++;
			}
			auto key = string_t(path.c_str() + start, i - start);
			PathComponent comp;
			comp.lookup_mode = VariantChildLookupMode::BY_KEY;
			comp.payload.key = std::move(key);
			components.push_back(std::move(comp));
			state = PathParsingState::BASE;
			break;
		}
		case PathParsingState::INDEX: {
			// Parse digits inside [ ]
			idx_t start = i;
			while (i < path.size() && isdigit(path[i])) {
				i++;
			}
			if (i == start || i >= path.size() || path[i] != ']') {
				throw BinderException("Invalid index in path: %s", path);
			}
			uint32_t index = std::stoul(path.substr(start, i - start));
			i++; // skip ']'
			PathComponent comp;
			comp.lookup_mode = VariantChildLookupMode::BY_INDEX;
			comp.payload.index = index;
			components.push_back(std::move(comp));
			state = PathParsingState::BASE;
			break;
		}
		}
	}
	return components;
}

VariantExtract::BindData::BindData(const string &constant_path) : FunctionData(), constant_path(constant_path) {
}

unique_ptr<FunctionData> VariantExtract::BindData::Copy() const {
	return make_uniq<BindData>(constant_path);
}
bool VariantExtract::BindData::Equals(const FunctionData &other) const {
	auto &bind_data = other.Cast<BindData>();
	return bind_data.constant_path == constant_path;
}

unique_ptr<FunctionData> VariantExtract::Bind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw BinderException("'variant_extract' expects two arguments, VARIANT column and VARCHAR path");
	}
	auto &path = *arguments[1];
	if (path.return_type.id() != LogicalTypeId::VARCHAR) {
		throw BinderException("'variant_extract' expects the second argument to be of type VARCHAR, not %s",
		                      path.return_type.ToString());
	}
	string constant_path;
	if (!TryParseConstantPattern(context, path, constant_path)) {
		throw BinderException("'variant_extract' expects the second argument (path) to be a constant expression");
	}
	return make_uniq<VariantExtract::BindData>(constant_path);
}

//! FIXME: it could make sense to allow a third argument: 'default'
//! This can currently be achieved with COALESCE(TRY(<extract method>), 'default')
void VariantExtract::Func(DataChunk &input, ExpressionState &state, Vector &result) {
	auto count = input.size();

	D_ASSERT(input.ColumnCount() == 2);
	auto &variant = input.data[0];
	D_ASSERT(variant.GetType() == CreateVariantType());

	auto &path = input.data[1];
	D_ASSERT(path.GetType().id() == LogicalTypeId::VARCHAR);
	//! FIXME: do we want to assert that this is a constant, or allow different paths per row?
	D_ASSERT(path.GetVectorType() == VectorType::CONSTANT_VECTOR);

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant, count, source_format);

	//! Path either contains array indices or object keys
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		validity.SetInvalid(i);
	}

	if (input.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

} // namespace duckdb
