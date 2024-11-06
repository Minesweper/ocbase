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

#include "sql/operator/logical_operator.h"
#include "storage/field/field.h"
#include "common/types.h"


class TableGetLogicalOperator : public LogicalOperator
{
public:
  TableGetLogicalOperator(Table *table, ReadWriteMode mode, const std::vector<Field> &fields);
  virtual ~TableGetLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::TABLE_GET; }

  Table        *table() const { return table_; }
  ReadWriteMode read_write_mode() const { return mode_; }

  void set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs);
  auto predicates() -> std::vector<std::unique_ptr<Expression>> & { return predicates_; }

  bool readonly() const { return (!(static_cast<bool>(mode_))); }

private:
  Table        *table_ = nullptr;
  ReadWriteMode mode_  = ReadWriteMode::READ_WRITE;
  std::vector<Field> fields_;


  std::vector<std::unique_ptr<Expression>> predicates_;
};
