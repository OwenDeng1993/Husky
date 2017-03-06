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

#include <string>
#include <vector>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "io/input/elasticsearch_connector/http.h"
#include "io/input/inputformat_base.hpp"

namespace husky {
namespace io {

class ElasticsearchInputFormat final : public InputFormatBase {
   public:
    typedef std::string RecordT;
    ElasticsearchInputFormat(std::string port = "9200");
    virtual ~ElasticsearchInputFormat();
    virtual bool is_setup() const;
    bool isActive();
    int find_shard();

    void set_query(const std::string& index, const std::string& type, const std::string& query, int local_id = 0);

    bool get_document(const std::string& index, const std::string& type, const std::string& id);

    int scan_fully(const std::string& index, const std::string& type, const std::string& query, int scrollSize,
                   int local_id = 0);

    virtual bool next(RecordT& ref);

    void read(boost::property_tree::ptree jresult, bool is_clear = true);

   protected:
    bool need_auth_ = false;
    boost::property_tree::ptree result;
    std::string node_;
    std::string node_id;
    std::string index_;
    std::string type_;
    std::string id_;
    std::string query_;
    std::string shard_;
    std::string router_;
    std::vector<RecordT> records_vector_;

    /// HTTP Connexion module.
    HTTP http_conn_;
};

}  // namespace io
}  // namespace husky
