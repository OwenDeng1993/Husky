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
#include "io/output/elasticsearch_outputformat.hpp"

namespace husky {
namespace io {

ElasticsearchOutputFormat::ElasticsearchOutputFormat(std::string port)
    : http_conn_(
          husky::Context::get_worker_info().get_hostname(husky::Context::get_worker_info().get_process_id()) + ":" + port, true) {
    if (!isActive())
        EXCEPTION("Cannot create engine, database is not active.");
    // geting the local node_id from the elasticsearch
    is_setup_ = 1;
    std::ostringstream oss;
    oss << "_nodes/_local";
    boost::property_tree::ptree msg;
    http_conn_.get(oss.str().c_str(), 0, &msg);
    node_id = msg.get_child("main").get_child("nodes").begin()->first;
    records_vector_.clear();
    bound_ = 1024;
}

ElasticsearchOutputFormat::~ElasticsearchOutputFormat() {}

bool ElasticsearchOutputFormat::is_setup() const { return (is_setup_); }

bool ElasticsearchOutputFormat::isActive() {
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

bool ElasticsearchOutputFormat::set_index(const std::string& index, const std::string& type,
                                          const boost::property_tree::ptree& content) {
    index_ = index;
    type_ = type;
    std::stringstream url;
    std::stringstream data;
    write_json(data, content);
    boost::property_tree::ptree result;
    url << index_ << "/" << type_ << "/";
    http_conn_.post(url.str().c_str(), data.str().c_str(), &result);
    return true;
}

bool ElasticsearchOutputFormat::set_index(const std::string& index, const std::string& type, const std::string& id,
                                          const boost::property_tree::ptree& content) {
    index_ = index;
    type_ = type;
    id_ = id;
    std::stringstream url;
    std::stringstream data;
    write_json(data, content);
    boost::property_tree::ptree result;
    url << index_ << "/" << type_ << "/" << id_;
    http_conn_.put(url.str().c_str(), data.str().c_str(), &result);
    return true;
}

bool ElasticsearchOutputFormat::bulk_add(const std::string& opt, const std::string& index, const std::string& type,
                                         const std::string& id, const std::string& content) {
    index_ = index;
    type_ = type;
    id_ = id;
    opt_ = opt;
    data << "{\"" << opt_ << "\":{\"_index\":\"" << index_ + "\",\"_type\":\"" << type_ << "\",\"_id\":\"" << id_
         << "\"}}" << std::endl;
    records_vector_.push_back(content);
    data << content << std::endl;
    if (bulk_is_full())
        bulk_flush();
    return true;
}

bool ElasticsearchOutputFormat::bulk_is_full() {
    if (records_vector_.size() >= bound_)
        return true;
    return false;
}

void ElasticsearchOutputFormat::bulk_flush() {
    if (records_vector_.empty())
        return;
    records_vector_.clear();
    http_conn_.post("/_bulk", data.str().c_str(), &result);
    data.clear();
    data.str("");
}

}  // namespace io
}  // namespace husky
