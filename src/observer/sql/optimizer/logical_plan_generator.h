/* Copyright (c) 2023 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/08/16.
//

#pragma once

#include <memory>

#include "common/rc.h"

class Stmt;
class CalcStmt;
class SelectStmt;
class FilterStmt;
class InsertStmt;
class DeleteStmt;
class ExplainStmt;
class LogicalOperator;

class LogicalPlanGenerator
{
public:
  LogicalPlanGenerator()          = default;
  virtual ~LogicalPlanGenerator() = default;

  static RC create(Stmt* stmt, std::unique_ptr<LogicalOperator>& logical_operator) {
    RC rc = RC::SUCCESS;
    switch (stmt->type()) {
      case StmtType::CALC: {
        CalcStmt *calc_stmt = static_cast<CalcStmt *>(stmt);

        rc = create_plan(calc_stmt, logical_operator);
      } break;

      case StmtType::SELECT: {
        SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);

        rc = create_plan(select_stmt, logical_operator);
      } break;

      case StmtType::INSERT: {
        InsertStmt *insert_stmt = static_cast<InsertStmt *>(stmt);

        rc = create_plan(insert_stmt, logical_operator);
      } break;

      case StmtType::DELETE: {
        DeleteStmt *delete_stmt = static_cast<DeleteStmt *>(stmt);

        rc = create_plan(delete_stmt, logical_operator);
      } break;

      case StmtType::EXPLAIN: {
        ExplainStmt *explain_stmt = static_cast<ExplainStmt *>(stmt);

        rc = create_plan(explain_stmt, logical_operator);
      } break;
      default: {
        rc = RC::UNIMPLENMENT;
      }
    }
    return rc;
  }

private:
  RC create_plan(CalcStmt *calc_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(SelectStmt *select_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(FilterStmt *filter_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(InsertStmt *insert_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(DeleteStmt *delete_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(ExplainStmt *explain_stmt, std::unique_ptr<LogicalOperator> &logical_operator);

  RC create_group_by_plan(SelectStmt *select_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
};