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

#include <cstring>
#include <iostream>
#include <list>
#include <sstream>
#include <string>
#include <vector>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "core/context.hpp"
#include "io/input/elasticsearch_inputformat.hpp"

namespace husky {
namespace io {

ElasticsearchInputFormat::ElasticsearchInputFormat()
    : http_conn_(
          husky::Context::get_worker_info().get_hostname(husky::Context::get_worker_info().get_process_id()) + ":9200",
          true) {
    if (!isActive())
        EXCEPTION("Cannot create engine, database is not active.");
    // geting the local node_id from the elasticsearch
    is_setup_ = 1;
    std::ostringstream oss;
    oss << "_nodes/_local";
    boost::property_tree::ptree msg;
    http_conn_.get(oss.str().c_str(), 0, &msg);
    node_id = msg.get_child("main").get_child("nodes").begin()->first;
}

ElasticsearchInputFormat::~ElasticsearchInputFormat() { records_vector_.clear(); }

bool ElasticsearchInputFormat::is_setup() const { return (is_setup_); }

bool ElasticsearchInputFormat::isActive() {
    boost::property_tree::ptree root;

    try {
        http_conn_.get(0, 0, &root);
    } catch (Exception& e) {
        printf("get(0) failed in ElasticSearch::isActive(). Exception caught: %s\n", e.what());
        return false;
    } catch (std::exception& e) {
        printf("get(0) failed in ElasticSearch::isActive(). std::exception caught: %s\n", e.what());
        return false;
    } catch (...) {
        printf("get(0) failed in ElasticSearch::isActive().\n");
        return false;
    }

    if (root.empty())
        return false;

    if (root.get<int>("status") != 200) {
        printf("Status is not 200. Cannot find Elasticsearch Node.\n");
        return false;
    }

    return true;
}

int ElasticsearchInputFormat::find_shard() {
    std::stringstream url;
    url << index_ << "/_search_shards?";
    boost::property_tree::ptree obj;
    http_conn_.get(url.str().c_str(), 0, &obj);
    boost::property_tree::ptree arr = obj.get_child("main").get_child("shards");
    int shard_num = 0;
    for (auto it = arr.begin(); it != arr.end(); ++it) {
        auto iter(it->second);
        for (auto it_(iter.begin()); it_ != (iter).end(); ++it_) {
            boost::property_tree::ptree obj_ = (it_->second);
            std::string _node = obj_.get<std::string>("node");
            std::string _pri = obj_.get<std::string>("primary");
            if (_node == node_id && _pri == "true") {
                shard_ = shard_ + "," + obj_.get<std::string>("shard");
                shard_num++;
            }
        }
    }
    return shard_num;
}

void ElasticsearchInputFormat::set_query(const std::string& index, const std::string& type, const std::string& query,
                                         int local_id) {
    index_ = index;
    type_ = type;
    query_ = query;
    std::stringstream url;
    if (husky::Context::get_local_tid() != local_id)
        return;
    int shard_number = find_shard();
    url << index_ << "/" << type_ << "/_search?preference=_shards:" << shard_ << ";_only_node:" << node_id;
    http_conn_.post(url.str().c_str(), query_.c_str(), &result);
    if (result.get_child("main").empty()) {
        std::cout << url.str() << " -d " << query << std::endl;
        EXCEPTION("Search failed.");
    }

    if (result.get_child("main").get<bool>("timed_out")) {
        EXCEPTION("Search timed out.");
    }
    ElasticsearchInputFormat::read(result);
}

bool ElasticsearchInputFormat::get_document(const std::string& index, const std::string& type, const std::string& id) {
    index_ = index;
    type_ = type;
    id_ = id;
    std::stringstream url;
    url << index_ << "/" << type_ << "/" << id_;
    http_conn_.get(url.str().c_str(), 0, &result);
    boost::property_tree::ptree pt = result.get_child("main");
    std::stringstream ss;
    write_json(ss, pt);
    records_vector_.clear();
    records_vector_.push_back(ss.str());
    return pt.get<bool>("found");
}

int ElasticsearchInputFormat::scan_fully(const std::string& index, const std::string& type, const std::string& query,
                                         int scrollSize, int local_id) {
    index_ = index;
    type_ = type;
    query_ = query;
    std::stringstream scrollUrl;
    if (husky::Context::get_local_tid() != local_id)
        return 0;
    int shard_number = find_shard();
    if (shard_number == 0)
        return 0;
    scrollUrl << index << "/" << type << "/_search?preference=_shards:" << shard_ << ";_only_node:" << node_id
              << "&search_type=scan&scroll=10m&size=" << scrollSize;
    boost::property_tree::ptree scrollObject;
    http_conn_.post(scrollUrl.str().c_str(), query_.c_str(), &scrollObject);
    if (scrollObject.get_child("main").get_child("hits").empty())
        EXCEPTION("Result corrupted, no member \"hits\".");
    if (!scrollObject.get_child("main").get_child("hits").get<int>("total"))
        EXCEPTION("Result corrupted, no member \"total\" nested in \"hits\".");
    int total = scrollObject.get_child("main").get_child("hits").get<int>("total");
    std::string scrollId = scrollObject.get_child("main").get<std::string>("_scroll_id");
    int count = 0;
    records_vector_.clear();

    while (count < total) {
        boost::property_tree::ptree result;
        http_conn_.rawpost("_search/scroll?scroll=10m", scrollId.c_str(), &result);
        scrollId = result.get_child("main").get<std::string>("_scroll_id");
        read(result, false);
        for (auto it = result.get_child("main").get_child("hits").get_child("hits").begin();
             it != result.get_child("main").get_child("hits").get_child("hits").end(); ++it)
            ++count;
    }
    if (count != total)
        EXCEPTION("Result corrupted, total is different from count.");
    return total;
}

void ElasticsearchInputFormat::read(boost::property_tree::ptree jresult, bool is_clear) {
    if (!records_vector_.empty() && is_clear)
        records_vector_.clear();
    boost::property_tree::ptree pt = jresult.get_child("main").get_child("hits").get_child("hits");
    for (auto it = pt.begin(); it != pt.end(); ++it) {
        std::stringstream ss;
        write_json(ss, it->second);
        records_vector_.push_back(ss.str());
    }
}

bool ElasticsearchInputFormat::next(RecordT& ref) {
    if (!records_vector_.empty()) {
        ref = records_vector_.back();
        records_vector_.pop_back();
        return true;
    }
    return false;
}

}  // namespace io
}  // namespace husky
