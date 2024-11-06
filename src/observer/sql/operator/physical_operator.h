/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/6/7.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/rc.h"
#include "sql/expr/tuple.h"

class Record;
class TupleCellSpec;
class Trx;


enum class PhysicalOperatorType
{
  TABLE_SCAN,
  TABLE_SCAN_VEC,
  INDEX_SCAN,
  NESTED_LOOP_JOIN,
  EXPLAIN,
  PREDICATE,
  PREDICATE_VEC,
  PROJECT,
  PROJECT_VEC,
  CALC,
  STRING_LIST,
  DELETE,
  INSERT,
  UPDATE,
  CREATE_TABLE,
  GROUP_BY,
  SCALAR_GROUP_BY,
  HASH_GROUP_BY,
  GROUP_BY_VEC,
  ORDER_BY,
  AGGREGATE_VEC,
  EXPR_VEC,
};


class PhysicalOperator
{
public:
  PhysicalOperator() = default;

  virtual ~PhysicalOperator() = default;


  virtual std::string name() const;
  virtual std::string param() const;

  virtual PhysicalOperatorType type() const = 0;

  virtual RC open(Trx *trx) = 0;
  virtual RC next() { return RC::UNIMPLENMENT; }
  virtual RC next(Chunk &chunk) { return RC::UNIMPLENMENT; }
  virtual RC close() = 0;

  virtual Tuple *current_tuple() { return nullptr; }

  virtual RC tuple_schema(TupleSchema &schema) const { return RC::UNIMPLENMENT; }

  void add_child(std::unique_ptr<PhysicalOperator> oper) { children_.emplace_back(std::move(oper)); }

  std::vector<std::unique_ptr<PhysicalOperator>> &children() { return children_; }

  void set_parent_tuple(const Tuple *tp)
  {
    parent_tuple_ = tp;
    for (auto &ch : children_) {
      ch->set_parent_tuple(tp);
    }
  }

protected:
  std::vector<std::unique_ptr<PhysicalOperator>> children_;
  const Tuple                                    *parent_tuple_ = 0;
};
