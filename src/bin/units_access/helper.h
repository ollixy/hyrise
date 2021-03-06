// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#pragma once

#include <json.h>
#include <gtest/gtest.h>
#include <helper/HwlocHelper.h>
#include <helper/types.h>
#include <storage/AbstractTable.h>

namespace hyrise {
namespace access {

//  Joins specified columns of a single table using hash join.
storage::c_atable_ptr_t hashJoinSameTable(storage::atable_ptr_t table, field_list_t &columns);

//  Used for message chaining to improve code readability when building edges maps
class EdgesBuilder {
  typedef std::pair<std::string, std::string> edge_t;
  typedef std::vector<edge_t> edges_map_t;
  typedef std::vector< std::pair<std::string, std::string> >::const_iterator
  edges_map_iterator;
  edges_map_t edges;
 public:
  EdgesBuilder() : edges(edges_map_t()) {}
  ~EdgesBuilder() {}
  EdgesBuilder &clear();
  EdgesBuilder &appendEdge(const std::string &src, const std::string &dst);
  Json::Value getEdges() const;
};

storage::c_atable_ptr_t sortTable(storage::c_atable_ptr_t table);

bool isEdgeEqual(
    const Json::Value &edges,
    const unsigned position,
    const std::string &src,
    const std::string &dst);


class ParameterValue;
typedef std::shared_ptr<ParameterValue> value_ptr_t;
typedef std::map<std::string, value_ptr_t> parameter_map_t;

class ParameterValue {
 public:
  std::string toString() const {
    return _value;
  }
 protected:
   std::string _value;
};

class FloatParameterValue : public ParameterValue {
 public:
  FloatParameterValue(hyrise_float_t value) {
    std::ostringstream os;
    os << value;
    _value = os.str();
  }
};

class IntParameterValue : public ParameterValue {
 public:
  IntParameterValue(hyrise_int_t value, size_t width = 0) {
    std::ostringstream os;
    os << std::right << std::setfill('0') << std::setw(width) << value;
    _value = os.str();
  }
};

class StringParameterValue : public ParameterValue {
 public:
  StringParameterValue(hyrise_string_t value) {
    _value = '\"' + value + '\"';
  }
};

void setParameteri(parameter_map_t& map, const std::string& name, int value, size_t width = 0);
void setParameterf(parameter_map_t& map, const std::string& name, float value);
void setParameters(parameter_map_t& map, const std::string& name, const std::string& value);

std::string loadFromFile(const std::string& path);
std::string loadParameterized(const std::string &path, const parameter_map_t& params);

storage::c_atable_ptr_t executeAndWait(
    std::string httpQuery,
    hyrise::tx::transaction_id_t* tid = nullptr,
    size_t poolSize = getNumberOfCoresOnSystem(),
    std::string *evt = nullptr);

} } // namespace hyrise::access
