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
// Created by Wangyunlai on 2022/12/07.
//

#pragma once

#include <memory>
#include <vector>

#include "sql/expr/expression.h"


enum class LogicalOperatorType
{
  CALC,
  TABLE_GET,   
  PREDICATE,   
  PROJECTION,  
  JOIN,        
  INSERT,      
  UPDATE,
  DELETE,      
  EXPLAIN,    
  GROUP_BY,
  ORDER_BY,
  CREATE_TABLE,
};


class LogicalOperator
{
public:
  LogicalOperator() = default;
  virtual ~LogicalOperator();

  virtual LogicalOperatorType type() const = 0;

  void        add_child(std::unique_ptr<LogicalOperator> oper);
  auto        children() -> std::vector<std::unique_ptr<LogicalOperator>>        &{ return children_; }
  auto        expressions() -> std::vector<std::unique_ptr<Expression>>        &{ return expressions_; }
  static bool can_generate_vectorized_operator(const LogicalOperatorType &type);

protected:
  std::vector<std::unique_ptr<LogicalOperator>> children_;
  std::vector<std::unique_ptr<Expression>> expressions_;
};
