// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_BIN_UNITS_ACCESS_HELPER_H_
#define SRC_BIN_UNITS_ACCESS_HELPER_H_

#include <json.h>
#include <gtest/gtest.h>
#include <helper/HwlocHelper.h>
#include <helper/types.h>
#include <storage/AbstractTable.h>

//  Joins specified columns of a single table using hash join.
hyrise::storage::c_atable_ptr_t hashJoinSameTable(
    hyrise::storage::atable_ptr_t table,
    field_list_t &columns);

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

hyrise::storage::c_atable_ptr_t sortTable(hyrise::storage::c_atable_ptr_t table);

bool isEdgeEqual(
    const Json::Value &edges,
    const unsigned position,
    const std::string &src,
    const std::string &dst);

namespace {
  template <typename T>
    std::string toStr(const T& t) {
    std::ostringstream os;
    os << t;
    return os.str();
  }
} // namespace

class ParameterValue;
typedef std::shared_ptr<ParameterValue> value_ptr_t;
typedef std::map<std::string, value_ptr_t> parameter_map_t;

class ParameterValue {
 public:
  virtual std::string toString() const = 0;
};

class FloatParameterValue : public ParameterValue {
 public:
  FloatParameterValue(hyrise_float_t value) : _value(value) {}
  std::string toString() const { return toStr(_value); }
 
 private:
  const hyrise_float_t _value;
};

class IntParameterValue : public ParameterValue {
 public:
  IntParameterValue(hyrise_int_t value) : _value(value) {}
  std::string toString() const { return toStr(_value); }
 
 private:
  const hyrise_int_t _value;
};

class StringParameterValue : public ParameterValue {
 public:
  StringParameterValue(hyrise_string_t value) : _value("\"" + value + "\"") {}
  std::string toString() const { return _value; }
 
 private:
  const hyrise_string_t _value;
};

void setParameteri(parameter_map_t& map, const std::string& name, int value);
void setParameterf(parameter_map_t& map, const std::string& name, float value);
void setParameters(parameter_map_t& map, const std::string& name, const std::string& value);

std::string loadFromFile(const std::string& path);
std::string loadParameterized(const std::string &path, const parameter_map_t& params);

hyrise::storage::c_atable_ptr_t executeAndWait(
    std::string httpQuery,
    hyrise::tx::transaction_id_t* tid = nullptr,
    size_t poolSize = getNumberOfCoresOnSystem(),
    std::string *evt = nullptr);

#endif  // SRC_BIN_UNITS_ACCESS_HELPER_H_
