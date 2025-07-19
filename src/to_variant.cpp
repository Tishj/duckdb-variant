#include "variant_functions.hpp"
#include "variant_extension.hpp"

namespace duckdb {

namespace {

template <class T>
static uint8_t GetVarintSize(T val) {
	uint8_t res = 0;
	do {
		val >>= 7;
		res++;
	} while (val != 0);
	return res;
}

struct OffsetData {
	uint32_t *GetKeys(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(*offsets.data[0]);
	}
	uint32_t *GetChildren(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(*offsets.data[1]);
	}
	uint32_t *GetValues(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(*offsets.data[2]);
	}
	uint32_t *GetBlob(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(*offsets.data[3]);
	}
}

} // namespace

static void IncreaseOffset(uint32_t *offset_data, idx_t count, idx_t increment, SelectionVector *selvec) {
	for (idx_t i = 0; i < count; i++) {
		offset_data[selvec ? selvec->get_index(i) : i] += increment;
	}
}

//! * @param source The Vector of arbitrary type to process
//! * @param offsets The offsets to gather per row
//! * @param count The size of the source vector
//! * @param selvec The selection vector from i (< count) to the index in the offsets Vectors
static void CollectOffsets(Vector &source, DataChunk &offsets, idx_t count, SelectionVector *selvec) {
	auto &type = source.GetType();
	auto physical_type = type.InternalType();
	auto blob_data = OffsetData::GetBlob(offsets);
	auto values_data = OffsetData::GetChildren(offsets);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(total_length, source_format);
	if (type.IsNested()) {
		auto keys_data = OffsetData::GetKeys(offsets);
		auto child_data = OffsetData::GetChildren(offsets);
		if (physical_type == PhysicalType::LIST) {
			auto source_data = source_format.GetData<list_entry_t>(source_format);

			//! Create a selection vector that maps to the right row for the child vector
			auto list_size = ListVector::GetListSize(source);
			SelectionVector new_selection(0, list_size);
			for (idx_t i = 0; i < count; i++) {
				auto index = source_format.sel->get_index(i);
				auto result_index = selvec ? selvec->get_index(i) ? i;
				auto &entry = source_data[index];

				blob_data[result_index] += GetVarintSize(entry.length);
				blob_data[result_index] += GetVarintSize(children_data[result_index]);
				values_data[result_index]++;
				children_data[result_index] += entry.length;
				for (idx_t child_idx = 0; child_idx < entry.length; child_idx++) {
					new_selection.set_index(child_idx + entry.offset, result_index);
				}
			}
			auto &entry = ListVector::GetEntry(source);
			CollectOffsets(entry, offsets, list_size, &new_selection);
		} else if (physical_type == PhysicalType::STRUCT) {
			auto &children =  StructVector::GetEntries(source);

			for (idx_t i = 0; i < count; i++) {
				auto result_index = selvec ? selvec->get_index(i) : i;
				values_data[result_index]++;
				blob_data[result_index] += GetVarintSize(children.size());
				blob_data[result_index] += children_data[result_index];
				children_data[result_index] += children.size();
				keys_data[result_index] += children.size();
			}

			for (auto &child_ptr : children) {
				auto &child = *child_ptr;
				CollectOffsets(child, offsets, count, selvec);
			}
		} else {
			throw NotImplementedException("Can't collect offsets for physical type '%s'", EnumUtil::ToString(physical_type));
		}

	} else if (physical_type == PhysicalType::VARCHAR) {
		auto source_data = source_format.GetData<string_t>(source_format);
		for (idx_t i = 0; i < count; i++) {
			auto index = source_format.sel->get_index(i);
			auto &val = source_data[index];

			auto result_index = selvec ? selvec->get_index(i) : i;
			blob_data[result_index] += GetVarintSize(val.GetSize());
			blob_data[result_index] += val.GetSize();
			values_data[result_index]++;
		}
	} else {
		auto type_id_size = GetTypeIdSize(physical_type);
		IncreaseOffset(blob_data, count, type_id_size, selvec);
		IncreaseOffset(values_data, count, 1, selvec);
	}
}

static LogicalType OffsetsType() {
	return LogicalType::STRUCT({
		{"keys_offset", LogicalTypeId::UINTEGER},
		{"children_offset", LogicalTypeId::UINTEGER},
		{"values_offset", LogicalTypeId::UINTEGER},
		{"blob_offset", LogicalTypeId::UINTEGER}
	});
}

static void InitializeOffsets(DataChunk &offsets, idx_t count) {
	offsets.Initialize(Allocator::DefaultAllocator(), {
		LogicalType::UINTEGER,
		LogicalType::UINTEGER,
		LogicalType::UINTEGER,
		LogicalType::UINTEGER
	}, count);

	auto keys = FlatVector::GetData<uint32_t>(*offsets.data[0]);
	auto children = FlatVector::GetData<uint32_t>(*offsets.data[1]);
	auto values = FlatVector::GetData<uint32_t>(*offsets.data[2]);
	auto blob = FlatVector::GetData<uint32_t>(*offsets.data[3]);
	for (idx_t i = 0; i < count; i++) {
		keys[i] = 0;
		children[i] = 0;
		values[i] = 0;
		blob[i] = 0;
	}
}

static bool ConvertVector(Vector &source, Vector &result, DataChunk &offsets, idx_t count, SelectionVector *selvec) {
	auto &type = source.GetType();
	auto physical_type = type.InternalType();
	auto blob_data = OffsetData::GetBlob(offsets);
	auto values_data = OffsetData::GetChildren(offsets);

	if (type.IsNested()) {

	} else if (physical_type == PhysicalType::VARCHAR) {

	} else {

	}
	return true;
}

void InitializeVariants(DataChunk &offsets, Vector &result, idx_t count) {
	auto &keys = VariantVector::GetKeys(result);
	auto keys_data = ListVector::GetData(keys);

	auto &children = VariantVector::GetChildren(result);
	auto children_data = ListVector::GetData(children);

	auto &values = VariantVector::GetValues(result);
	auto values_data = ListVector::GetData(values);

	auto &blob = VariantVector::GetValue(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);

	idx_t keys_offset = 0;
	idx_t children_offset = 0;
	idx_t values_offset = 0;

	auto &keys_sizes = *offsets.data[0];
	auto &children_sizes = *offsets.data[1];
	auto &values_sizes = *offsets.data[2];
	auto &blob_sizes = *offsets.data[3];
	for (idx_t i = 0; i < count; i++) {
		auto &keys_entry = keys_data[i];
		auto &children_entry = children_data[i];
		auto &values_entry = values_data[i];

		//! keys
		keys_entry.length = keys_sizes[i];
		keys_entry.offset = keys_offset;
		keys_offset += keys_entry.length;

		//! children
		children_entry.length = children_sizes[i];
		children_entry.offset = children_offset;
		children_offset += children_entry.length;

		//! values
		values_entry.length = values_sizes[i];
		values_entry.offset = values_offset;
		values_offset += values_entry.length;

		//! value
		blob_data[i] = StringVector::EmptyString(blob, blob_sizes[i]);
	}
	//! Reserve for the children of the lists
	ListVector::Reserve(keys, keys_offset);
	ListVector::Reserve(children, children_offset);
	ListVector::Reserve(values, values_offset);

	//! Set list sizes
	ListVector::SetListSize(keys, keys_offset);
	ListVector::SetListSize(children, children_offset);
	ListVector::SetListSize(values, values_offset);
}

bool VariantFunctions::CastToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	DataChunk offsets;
	InitializeOffsets(offsets, count);
	CollectOffsets(source, offsets, count, nullptr);

	InitializeVariant(offsets, count);
	//! Reset the offsets to 0
	InitializeOffsets(offsets, count);
	return ConvertVector(input, res, offsets, result, nullptr);
}

} // namespace duckdb
