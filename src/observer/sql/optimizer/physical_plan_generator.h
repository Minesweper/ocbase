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
// Created by Wangyunlai on 2022/12/14.
//

#pragma once

#include <utility>
#include <memory>
#include <vector>

#include "common/rc.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/operator/aggregate_vec_physical_operator.h"
#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/calc_physical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/delete_physical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/explain_physical_operator.h"
#include "sql/operator/expr_vec_physical_operator.h"
#include "sql/operator/group_by_vec_physical_operator.h"
#include "sql/operator/index_scan_physical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/insert_physical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/join_physical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/predicate_physical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/project_physical_operator.h"
#include "sql/operator/project_vec_physical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/table_scan_physical_operator.h"
#include "sql/operator/group_by_logical_operator.h"
#include "sql/operator/group_by_physical_operator.h"
#include "sql/operator/hash_group_by_physical_operator.h"
#include "sql/operator/scalar_group_by_physical_operator.h"
#include "sql/operator/table_scan_vec_physical_operator.h"


class TableGetLogicalOperator;
class PredicateLogicalOperator;
class ProjectLogicalOperator;
class InsertLogicalOperator;
class DeleteLogicalOperator;
class ExplainLogicalOperator;
class JoinLogicalOperator;
class CalcLogicalOperator;
class GroupByLogicalOperator;


class PhysicalPlanGenerator
{
public:
  PhysicalPlanGenerator()          = default;
  virtual ~PhysicalPlanGenerator() = default;

  static RC create(LogicalOperator& logical_operator, std::unique_ptr<PhysicalOperator>& oper) {
    RC rc = RC::SUCCESS;

    switch (logical_operator.type()) {
      case LogicalOperatorType::CALC: {
        return create_plan(static_cast<CalcLogicalOperator &>(logical_operator), oper);
      } break;

      case LogicalOperatorType::TABLE_GET: {
        return create_plan(static_cast<TableGetLogicalOperator &>(logical_operator), oper);
      } break;

      case LogicalOperatorType::PREDICATE: {
        return create_plan(static_cast<PredicateLogicalOperator &>(logical_operator), oper);
      } break;

      case LogicalOperatorType::PROJECTION: {
        return create_plan(static_cast<ProjectLogicalOperator &>(logical_operator), oper);
      } break;

      case LogicalOperatorType::INSERT: {
        return create_plan(static_cast<InsertLogicalOperator &>(logical_operator), oper);
      } break;

      case LogicalOperatorType::DELETE: {
        return create_plan(static_cast<DeleteLogicalOperator &>(logical_operator), oper);
      } break;

      case LogicalOperatorType::EXPLAIN: {
        return create_plan(static_cast<ExplainLogicalOperator &>(logical_operator), oper);
      } break;

      case LogicalOperatorType::JOIN: {
        return create_plan(static_cast<JoinLogicalOperator &>(logical_operator), oper);
      } break;

      case LogicalOperatorType::GROUP_BY: {
        return create_plan(static_cast<GroupByLogicalOperator &>(logical_operator), oper);
      } break;

      default: {
        ASSERT(false, "unknown logical operator type");
        return RC::INVALID_ARGUMENT;
      }
    }
    return rc;
  }

  static RC create_vec(LogicalOperator& logical_operator, std::unique_ptr<PhysicalOperator>& oper) {
    RC rc = RC::SUCCESS;

    switch (logical_operator.type()) {
      case LogicalOperatorType::TABLE_GET: {
        return create_vec_plan(static_cast<TableGetLogicalOperator &>(logical_operator), oper);
      } break;
      case LogicalOperatorType::PROJECTION: {
        return create_vec_plan(static_cast<ProjectLogicalOperator &>(logical_operator), oper);
      } break;
      case LogicalOperatorType::GROUP_BY: {
        return create_vec_plan(static_cast<GroupByLogicalOperator &>(logical_operator), oper);
      } break;
      case LogicalOperatorType::EXPLAIN: {
        return create_vec_plan(static_cast<ExplainLogicalOperator &>(logical_operator), oper);
      } break;
      default: {
        return RC::INVALID_ARGUMENT;
      }
    }
    return rc;
  }

private:
  static RC create_plan(TableGetLogicalOperator &table_get_oper, std::unique_ptr<PhysicalOperator> &oper)
  {
    std::vector<std::unique_ptr<Expression>> &predicates = table_get_oper.predicates();
    // 看看是否有可以用于索引查找的表达式
    Table *table = table_get_oper.table();

    Index     *index      = nullptr;
    ValueExpr *value_expr = nullptr;
    for (auto &expr : predicates) {
      if (expr->type() == ExprType::COMPARISON) {
        auto comparison_expr = static_cast<ComparisonExpr *>(expr.get());
        // 简单处理，就找等值查询
        if (comparison_expr->comp() != EQUAL_TO) {
          continue;
        }

        std::unique_ptr<Expression> &left_expr  = comparison_expr->left();
        std::unique_ptr<Expression> &right_expr = comparison_expr->right();
        // 左右比较的一边最少是一个值
        if (left_expr->type() != ExprType::VALUE && right_expr->type() != ExprType::VALUE) {
          continue;
        }

        FieldExpr *field_expr = nullptr;
        if (left_expr->type() == ExprType::FIELD) {
          ASSERT(right_expr->type() == ExprType::VALUE, "right expr should be a value expr while left is field expr");
          field_expr = static_cast<FieldExpr *>(left_expr.get());
          value_expr = static_cast<ValueExpr *>(right_expr.get());
        } else if (right_expr->type() == ExprType::FIELD) {
          ASSERT(left_expr->type() == ExprType::VALUE, "left expr should be a value expr while right is a field expr");
          field_expr = static_cast<FieldExpr *>(right_expr.get());
          value_expr = static_cast<ValueExpr *>(left_expr.get());
        }

        if (field_expr == nullptr) {
          continue;
        }

        const Field &field = field_expr->field();
        index              = table->find_index_by_field(field.field_name());
        if (nullptr != index) {
          break;
        }
      }
    }

    if (index != nullptr) {
      ASSERT(value_expr != nullptr, "got an index but value expr is null ?");

      const Value               &value           = value_expr->get_value();
      IndexScanPhysicalOperator *index_scan_oper = new IndexScanPhysicalOperator(table,
          index,
          table_get_oper.read_write_mode(),
          &value,
          true /*left_inclusive*/,
          &value,
          true /*right_inclusive*/);

      index_scan_oper->set_predicates(std::move(predicates));
      oper = std::unique_ptr<PhysicalOperator>(index_scan_oper);
      LOG_TRACE("use index scan");
    } else {
      auto table_scan_oper = new TableScanPhysicalOperator(table, table_get_oper.read_write_mode());
      table_scan_oper->set_predicates(std::move(predicates));
      oper = std::unique_ptr<PhysicalOperator>(table_scan_oper);
      LOG_TRACE("use table scan");
    }

    return RC::SUCCESS;
  }

  static RC create_plan(PredicateLogicalOperator& pred_oper, std::unique_ptr<PhysicalOperator>& oper) {
    std::vector<std::unique_ptr<LogicalOperator>> &children_opers = pred_oper.children();
    ASSERT(children_opers.size() == 1, "predicate logical operator's sub oper number should be 1");

    LogicalOperator &child_oper = *children_opers.front();

    std::unique_ptr<PhysicalOperator> child_phy_oper;
    RC                                rc = create(child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create child operator of predicate operator. rc=%s", strrc(rc));
      return rc;
    }

    std::vector<std::unique_ptr<Expression>> &expressions = pred_oper.expressions();
    ASSERT(expressions.size() == 1, "predicate logical operator's children should be 1");

    std::unique_ptr<Expression> expression = std::move(expressions.front());
    oper = std::unique_ptr<PhysicalOperator>(new PredicatePhysicalOperator(std::move(expression)));
    oper->add_child(std::move(child_phy_oper));
    return rc;
  }

  static RC create_plan(ProjectLogicalOperator &project_oper, std::unique_ptr<PhysicalOperator> &oper) {
    std::vector<std::unique_ptr<LogicalOperator>> &child_opers = project_oper.children();

    std::unique_ptr<PhysicalOperator> child_phy_oper;

    RC rc = RC::SUCCESS;
    if (!child_opers.empty()) {
      LogicalOperator *child_oper = child_opers.front().get();

      rc = create(*child_oper, child_phy_oper);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
        return rc;
      }
    }

    auto project_operator = std::make_unique<ProjectPhysicalOperator>(std::move(project_oper.expressions()));
    if (child_phy_oper) {
      project_operator->add_child(std::move(child_phy_oper));
    }

    oper = std::move(project_operator);

    LOG_TRACE("create a project physical operator");
    return rc;
  }
  static RC create_plan(InsertLogicalOperator& insert_oper, std::unique_ptr<PhysicalOperator>& oper) {
    Table                  *table           = insert_oper.table();
    std::vector<std::vector<Value>>     &values          = insert_oper.values();
    InsertPhysicalOperator *insert_phy_oper = new InsertPhysicalOperator(table, std::move(values));
    oper.reset(insert_phy_oper);
    return RC::SUCCESS;
  }
  static RC create_plan(DeleteLogicalOperator& delete_oper, std::unique_ptr<PhysicalOperator>& oper) {
    std::vector<std::unique_ptr<LogicalOperator>> &child_opers = delete_oper.children();

    std::unique_ptr<PhysicalOperator> child_physical_oper;

    RC rc = RC::SUCCESS;
    if (!child_opers.empty()) {
      LogicalOperator *child_oper = child_opers.front().get();

      rc = create(*child_oper, child_physical_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
        return rc;
      }
    }

    oper = std::unique_ptr<PhysicalOperator>(new DeletePhysicalOperator(delete_oper.table()));

    if (child_physical_oper) {
      oper->add_child(std::move(child_physical_oper));
    }
    return rc;
  }
  static RC create_plan(ExplainLogicalOperator& explain_oper, std::unique_ptr<PhysicalOperator>& oper) {
    std::vector<std::unique_ptr<LogicalOperator>> &child_opers = explain_oper.children();

    RC rc = RC::SUCCESS;

    std::unique_ptr<PhysicalOperator> explain_physical_oper(new ExplainPhysicalOperator);
    for (std::unique_ptr<LogicalOperator> &child_oper : child_opers) {
      std::unique_ptr<PhysicalOperator> child_physical_oper;
      rc = create(*child_oper, child_physical_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
        return rc;
      }

      explain_physical_oper->add_child(std::move(child_physical_oper));
    }

    oper = std::move(explain_physical_oper);
    return rc;
  }
  static RC create_plan(JoinLogicalOperator& join_oper, std::unique_ptr<PhysicalOperator>& oper) {
    RC rc = RC::SUCCESS;

    std::vector<std::unique_ptr<LogicalOperator>> &child_opers = join_oper.children();
    if (child_opers.size() != 2) {
      LOG_WARN("join operator should have 2 children, but have %d", child_opers.size());
      return RC::INTERNAL;
    }

    std::unique_ptr<PhysicalOperator> join_physical_oper(new NestedLoopJoinPhysicalOperator);
    for (auto &child_oper : child_opers) {
      std::unique_ptr<PhysicalOperator> child_physical_oper;
      rc = create(*child_oper, child_physical_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create physical child oper. rc=%s", strrc(rc));
        return rc;
      }

      join_physical_oper->add_child(std::move(child_physical_oper));
    }

    oper = std::move(join_physical_oper);
    return rc;
  }
  static RC create_plan(CalcLogicalOperator& logical_oper, std::unique_ptr<PhysicalOperator>& oper) {
    RC rc = RC::SUCCESS;

    CalcPhysicalOperator *calc_oper = new CalcPhysicalOperator(std::move(logical_oper.expressions()));
    oper.reset(calc_oper);
    return rc;
  }
  static RC create_plan(GroupByLogicalOperator& groupby_oper, std::unique_ptr<PhysicalOperator>& oper) {
    std::vector<std::unique_ptr<LogicalOperator>> &child_opers = groupby_oper.children();
    std::unique_ptr<PhysicalOperator>         child_phy_oper;

    RC rc = RC::SUCCESS;
    if (!child_opers.empty()) {
      LogicalOperator *child_oper = child_opers.front().get();
      rc                          = create(*child_oper, child_phy_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create groupby logical operator's child physical operator. rc=%s", strrc(rc));
        return rc;
      }
    }

    GroupByPhysicalOperator *groupby_operator = new GroupByPhysicalOperator(std::move(groupby_oper.group_by_expressions()),
            std::move(groupby_oper.aggregate_expressions()),
        std::move(groupby_oper.field_exprs()));
    if (child_phy_oper) {
      groupby_operator->add_child(std::move(child_phy_oper));
    }

    oper = std::unique_ptr<PhysicalOperator>(groupby_operator);

    LOG_TRACE("create a groupby physical operator");
    return rc;
  }
  static RC create_vec_plan(ProjectLogicalOperator& project_oper, std::unique_ptr<PhysicalOperator>& oper) {
    std::vector<std::unique_ptr<LogicalOperator>> &child_opers = project_oper.children();

    std::unique_ptr<PhysicalOperator> child_phy_oper;

    RC rc = RC::SUCCESS;
    if (!child_opers.empty()) {
      LogicalOperator *child_oper = child_opers.front().get();
      rc                          = create_vec(*child_oper, child_phy_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
        return rc;
      }
    }

    auto project_operator = std::make_unique<ProjectVecPhysicalOperator>(std::move(project_oper.expressions()));

    if (child_phy_oper != nullptr) {
      std::vector<Expression *> expressions;
      for (auto &expr : project_operator->expressions()) {
        expressions.push_back(expr.get());
      }
      auto expr_operator = std::make_unique<ExprVecPhysicalOperator>(std::move(expressions));
      expr_operator->add_child(std::move(child_phy_oper));
      project_operator->add_child(std::move(expr_operator));
    }

    oper = std::move(project_operator);

    LOG_TRACE("create a project physical operator");
    return rc;
  }
  static RC create_vec_plan(TableGetLogicalOperator& table_get_oper, std::unique_ptr<PhysicalOperator>& oper) {
    std::vector<std::unique_ptr<Expression>> &predicates = table_get_oper.predicates();
    Table                                    *table      = table_get_oper.table();
    TableScanVecPhysicalOperator             *table_scan_oper =
        new TableScanVecPhysicalOperator(table, table_get_oper.read_write_mode());
    table_scan_oper->set_predicates(std::move(predicates));
    oper = std::unique_ptr<PhysicalOperator>(table_scan_oper);
    LOG_TRACE("use vectorized table scan");

    return RC::SUCCESS;
  }
  static RC create_vec_plan(GroupByLogicalOperator& logical_oper, std::unique_ptr<PhysicalOperator>& oper) {
    RC                                rc            = RC::SUCCESS;
    std::unique_ptr<PhysicalOperator> physical_oper = nullptr;
    if (logical_oper.group_by_expressions().empty()) {
      physical_oper = std::make_unique<AggregateVecPhysicalOperator>(std::move(logical_oper.aggregate_expressions()));
    } else {
      physical_oper = std::make_unique<GroupByVecPhysicalOperator>(
          std::move(logical_oper.group_by_expressions()), std::move(logical_oper.aggregate_expressions()));
    }

    ASSERT(logical_oper.children().size() == 1, "group by operator should have 1 child");

    LogicalOperator                  &child_oper = *logical_oper.children().front();
    std::unique_ptr<PhysicalOperator> child_physical_oper;
    rc = create_vec(child_oper, child_physical_oper);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to create child physical operator of group by(vec) operator. rc=%s", strrc(rc));
      return rc;
    }

    physical_oper->add_child(std::move(child_physical_oper));

    oper = std::move(physical_oper);
    return rc;

    return RC::SUCCESS;
  }
  static RC create_vec_plan(ExplainLogicalOperator& explain_oper, std::unique_ptr<PhysicalOperator>& oper) {
    std::vector<std::unique_ptr<LogicalOperator>> &child_opers = explain_oper.children();

    RC rc = RC::SUCCESS;
    // reuse `ExplainPhysicalOperator` in explain vectorized physical plan
    std::unique_ptr<PhysicalOperator> explain_physical_oper(new ExplainPhysicalOperator);
    for (std::unique_ptr<LogicalOperator> &child_oper : child_opers) {
      std::unique_ptr<PhysicalOperator> child_physical_oper;
      rc = create_vec(*child_oper, child_physical_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
        return rc;
      }

      explain_physical_oper->add_child(std::move(child_physical_oper));
    }

    oper = std::move(explain_physical_oper);
    return rc;
  }
};