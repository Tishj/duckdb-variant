#pragma once

#include "variant_functions.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

enum class VariantChildLookupMode : uint8_t { BY_KEY, BY_INDEX };

struct VariantUtils {
	template <VariantChildLookupMode MODE>
	static bool FindChildValues(RecursiveUnifiedVectorFormat &source, const PathComponent &component, optional_idx row,
	                            uint32_t *res, VariantNestedData *nested_data, idx_t count) {
		//! children
		auto &children = UnifiedVariantVector::GetChildren(source);
		auto children_data = children.GetData<list_entry_t>(children);

		//! value_ids
		auto &value_ids = UnifiedVariantVector::GetChildrenValueId(source);
		auto value_ids_data = value_ids.GetData<uint32_t>(value_ids);

		//! key_ids
		auto &key_ids = UnifiedVariantVector::GetChildrenKeyId(source);
		auto key_ids_data = key_ids.GetData<uint32_t>(key_ids);

		//! keys
		auto &keys = UnifiedVariantVector::GetKeys(source);
		auto keys_data = keys.GetData<list_entry_t>(keys);

		//! entry of the keys list
		auto &keys_entry = UnifiedVariantVector::GetKeysEntry(source);
		auto keys_entry_data = keys_entry.GetData<string_t>(keys_entry);

		for (idx_t i = 0; i < count; i++) {
			auto row_index = row.IsValid() ? row.GetIndex() : i;
			auto &children_list_entry = children_data[children.sel->get_index(row_index)];

			auto &nested_data_entry = nested_data[i];
			if (MODE == VariantChildLookupMode::BY_INDEX) {
				auto child_idx = component.payload.index;
				if (child_idx >= nested_data_entry.child_count) {
					//! The list is too small to contain this index
					return false;
				}
				auto children_index = children_list_entry.offset + nested_data_entry.children_idx + child_idx;
				auto value_id = value_ids_data[value_ids.sel->get_index(children_index)];
				res[i] = value_id;
				continue;
			}
			auto &keys_list_entry = keys_data[keys.sel->get_index(row_index)];
			bool found_child = false;
			for (idx_t child_idx = 0; child_idx < nested_data_entry.child_count; child_idx++) {
				auto children_index = children_list_entry.offset + nested_data_entry.children_idx + child_idx;
				auto value_id = value_ids_data[value_ids.sel->get_index(children_index)];

				auto key_id = key_ids_data[key_ids.sel->get_index(children_index)];
				auto key_index = keys_entry.sel->get_index(keys_list_entry.offset + key_id);
				auto &child_key = keys_entry_data[key_index];
				if (child_key == component.payload.key) {
					//! Found the key we're looking for
					res[i] = value_id;
					found_child = true;
					break;
				}
			}
			if (!found_child) {
				return false;
			}
		}
		return true;
	}
};

} // namespace duckdb
