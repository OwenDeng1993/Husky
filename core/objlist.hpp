// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost/random.hpp"

#include "base/assert.hpp"
#include "base/disk_store.hpp"
#include "base/exception.hpp"
#include "base/serialization.hpp"
#include "core/attrlist.hpp"
#include "core/channel/channel_destination.hpp"
#include "core/channel/channel_source.hpp"

namespace husky {

using base::BinStream;
using base::DiskStore;

class ObjListBase : public ChannelSource, public ChannelDestination {
   public:
    ObjListBase() : id_(s_counter) { s_counter++; }

    virtual ~ObjListBase() = default;

    ObjListBase(const ObjListBase&) = delete;
    ObjListBase& operator=(const ObjListBase&) = delete;

    ObjListBase(ObjListBase&&) = delete;
    ObjListBase& operator=(ObjListBase&&) = delete;

    // TODO(legend): Consider how to add the index of thread_id.
    inline std::string id2str() const { return "ObjList-" + std::to_string(id_); }

    inline size_t get_id() const { return id_; }

    virtual size_t get_size() const = 0;

   private:
    size_t id_;

    static thread_local size_t s_counter;
};

template <typename ObjT>
class ObjList : public ObjListBase {
   public:
    // TODO(all): should be protected. The list should be constructed by possibly Context
    ObjList() = default;

    virtual ~ObjList() {
        for (auto& it : this->attrlist_map)
            delete it.second;
        this->attrlist_map.clear();
    }

    ObjList(const ObjList&) = default;
    ObjList& operator=(const ObjList&) = default;

    ObjList(ObjList&&) = default;
    ObjList& operator=(ObjList&&) = default;

    std::vector<ObjT>& get_data() { return objlist_data_.data_; }
    std::vector<bool>& get_del_bitmap() { return del_bitmap_; }

    // Sort the objlist
    void sort() {
        auto& data = objlist_data_.data_;
        if (data.size() == 0)
            return;
        std::vector<int> order(this->get_size());
        for (int i = 0; i < order.size(); ++i)
            order[i] = i;
        // sort the permutation
        std::sort(order.begin(), order.end(),
                  [&](const size_t a, const size_t b) { return data[a].id() < data[b].id(); });
        // apply the permutation on all the attribute lists
        for (auto& it : this->attrlist_map)
            it.second->reorder(order);
        std::sort(data.begin(), data.end(), [](const ObjT& a, const ObjT& b) { return a.id() < b.id(); });
        hashed_objs_.clear();
        sorted_size_ = data.size();
    }

    // TODO(Fan): This will invalidate the object dict
    void deletion_finalize() {
        auto& data = objlist_data_.data_;
        if (data.size() == 0)
            return;
        size_t i = 0, j;
        // move i to the first empty place
        while (i < data.size() && !del_bitmap_[i])
            i++;

        if (i == data.size())
            return;

        for (j = data.size() - 1; j > 0; j--) {
            if (!del_bitmap_[j]) {
                data[i] = std::move(data[j]);
                // move j_th attribute to i_th for all attr lists
                for (auto& it : this->attrlist_map)
                    it.second->move(i, j);
                i += 1;
                // move i to the next empty place
                while (i < data.size() && !del_bitmap_[i])
                    i++;
            }
            if (i >= j)
                break;
        }
        data.resize(j);
        del_bitmap_.resize(j);
        for (auto& it : this->attrlist_map)
            it.second->resize(j);
        objlist_data_.num_del_ = 0;
        std::fill(del_bitmap_.begin(), del_bitmap_.end(), 0);
    }

    // Delete an object
    size_t delete_object(const ObjT* const obj_ptr) {
        // TODO(all): Decide whether we can remove this
        // if (unlikely(del_bitmap_.size() < data.size())) {
        //     del_bitmap_.resize(data.size());
        // }
        // lazy operation
        size_t idx = obj_ptr - &objlist_data_.data_[0];
        if (idx < 0 || idx >= objlist_data_.data_.size())
            throw base::HuskyException("ObjList<T>::delete_object error: index out of range");
        del_bitmap_[idx] = true;
        objlist_data_.num_del_ += 1;
        return idx;
    }

    // Find obj according to key
    // @Return a pointer to obj
    ObjT* find(const typename ObjT::KeyT& key) {
        auto& working_list = objlist_data_.data_;
        if (working_list.size() == 0)
            return nullptr;
        ObjT* start_addr = &working_list[0];
        int r = this->sorted_size_ - 1;
        int l = 0;
        int m = (r + l) / 2;

        while (l <= r) {
// __builtin_prefetch(start_addr+(m+1+r)/2, 0, 1);
// __builtin_prefetch(start_addr+(l+m-1)/2, 0, 1);
#ifdef ENABLE_LIST_FIND_PREFETCH
            __builtin_prefetch(&(start_addr[(m + 1 + r) / 2].id()), 0, 1);
            __builtin_prefetch(&(start_addr[(l + m - 1) / 2].id()), 0, 1);
#endif
            // __builtin_prefetch(&working_list[(m+1+r)/2], 0, 1);
            // __builtin_prefetch(&working_list[(l+m-1)/2], 0, 1);
            auto tmp = start_addr[m].id();
            if (tmp == key)
                return &working_list[m];
            else if (tmp < key)
                l = m + 1;
            else
                r = m - 1;
            m = (r + l) / 2;
        }

        // The object to find is not in the sorted part
        if ((sorted_size_ < objlist_data_.data_.size()) && (hashed_objs_.find(key) != hashed_objs_.end()))
            return &(objlist_data_.data_[hashed_objs_[key]]);
        return nullptr;
    }

    // Find the index of an obj
    size_t index_of(const ObjT* const obj_ptr) const { return objlist_data_.index_of(obj_ptr); }

    // Add an object
    size_t add_object(ObjT&& obj) {
        auto& data = objlist_data_.data_;
        size_t ret = hashed_objs_[obj.id()] = data.size();
        data.push_back(std::move(obj));
        del_bitmap_.push_back(0);
        return ret;
    }
    size_t add_object(const ObjT& obj) {
        auto& data = objlist_data_.data_;
        size_t ret = hashed_objs_[obj.id()] = data.size();
        data.push_back(obj);
        del_bitmap_.push_back(0);
        return ret;
    }

    // Check a certain position of del_bitmap_
    // @Return True if it's deleted
    bool get_del(size_t idx) const { return del_bitmap_[idx]; }

    // Create AttrList
    template <typename AttrT>
    AttrList<ObjT, AttrT>& create_attrlist(const std::string& attr_name, const AttrT& default_attr = {}) {
        if (attrlist_map.find(attr_name) != attrlist_map.end())
            throw base::HuskyException("ObjList<T>::create_attrlist error: name already exists");
        auto* attrlist = new AttrList<ObjT, AttrT>(&objlist_data_, default_attr);
        attrlist_map.insert({attr_name, attrlist});
        return (*attrlist);
    }

    // Get AttrList
    template <typename AttrT>
    AttrList<ObjT, AttrT>& get_attrlist(const std::string& attr_name) {
        if (attrlist_map.find(attr_name) == attrlist_map.end())
            throw base::HuskyException("ObjList<T>::get_attrlist error: AttrList does not exist");
        return (*static_cast<AttrList<ObjT, AttrT>*>(attrlist_map[attr_name]));
    }

    // Delete AttrList
    size_t del_attrlist(const std::string& attr_name) {
        if (attrlist_map.find(attr_name) != attrlist_map.end())
            delete attrlist_map[attr_name];
        return attrlist_map.erase(attr_name);
    }

    void migrate_attribute(BinStream& bin, const size_t idx) {
        if (!this->attrlist_map.empty())
            for (auto& item : this->attrlist_map)
                item.second->migrate(bin, idx);
    }

    void process_attribute(BinStream& bin, const size_t idx) {
        if (!this->attrlist_map.empty())
            for (auto& item : this->attrlist_map)
                item.second->process_bin(bin, idx);
    }

    inline size_t get_sorted_size() const { return sorted_size_; }
    inline size_t get_num_del() const { return objlist_data_.num_del_; }
    inline size_t get_hashed_size() const { return hashed_objs_.size(); }
    inline size_t get_size() const override { return objlist_data_.get_size(); }
    inline size_t get_vector_size() const { return objlist_data_.get_vector_size(); }
    inline ObjT& get(size_t i) { return objlist_data_.data_[i]; }

    bool write_to_disk() {
        DiskStore ds(id2str());
        BinStream bs;
        deletion_finalize();
        sort();
        bs << objlist_data_;
        this->clear_from_memory();
        return ds.write(std::move(bs));
    }

    void read_from_disk(const std::string& objlist_path) {
        DiskStore ds(objlist_path);
        BinStream bs = ds.read();
        objlist_data_.clear();
        bs >> objlist_data_;
        sorted_size_ = objlist_data_.data_.size();
        del_bitmap_.clear();
        del_bitmap_.resize(sorted_size_, false);
        hashed_objs_.clear();
    }

    void clear_from_memory() {
        std::vector<ObjT> tmp_obj;
        std::vector<ObjT>& data = this->get_data();
        data.swap(tmp_obj);

        std::vector<bool> tmp_bool;
        del_bitmap_.swap(tmp_bool);
    }

    size_t estimated_storage_size(const double sample_rate = 0.005) {
        if (this->get_vector_size() == 0)
            return 0;
        const size_t sample_num = this->get_vector_size() * sample_rate + 1;
        BinStream bs;

        // sample
        std::unordered_set<size_t> sample_container;
        boost::random::mt19937 generator;
        boost::random::uniform_real_distribution<double> distribution(0.0, 1.0);
        while (sample_container.size() < sample_num) {
            size_t index = distribution(generator) * objlist_data_.get_vector_size();
            sample_container.insert(index);
        }

        // log the size
        for (auto iter = sample_container.begin(); iter != sample_container.end(); ++iter)
            bs << objlist_data_.data_[*iter];

        std::vector<ObjT>& v = objlist_data_.data_;
        size_t ret = bs.size() * sizeof(char) * v.capacity() / sample_num;
        return ret;
    }

   protected:
    ObjListData<ObjT> objlist_data_;
    size_t sorted_size_ = 0;
    std::vector<bool> del_bitmap_;
    std::unordered_map<typename ObjT::KeyT, size_t> hashed_objs_;
    std::unordered_map<std::string, AttrListBase*> attrlist_map;
};
}  // namespace husky
