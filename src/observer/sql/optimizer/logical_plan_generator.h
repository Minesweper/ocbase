#pragma once

#include <memory>
#include <vector>
#include "common/rc.h"
#include "sql/optimizer/logical_plan_generator.h"

#include "sql/operator/logical_operator.h"
#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/create_table_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/update_logical_operator.h"
#include "sql/operator/group_by_logical_operator.h"
#include "sql/operator/orderby_logical_operator.h"

#include "sql/stmt/stmt.h"
#include "sql/stmt/calc_stmt.h"
#include "sql/stmt/create_table_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/explain_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/orderby_stmt.h"
#include"sql/stmt/groupby_stmt.h"


class Stmt;
class CalcStmt;
class SelectStmt;
class FilterStmt;
class InsertStmt;
class DeleteStmt;
class ExplainStmt;
class UpdateStmt;
class GroupByStmt;
class OrderByStmt;
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
        rc                  = create_plan(calc_stmt, logical_operator);
      } break;


      case StmtType::SELECT: {
        SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);
        rc                      = create_plan(select_stmt, logical_operator);
      } break;

      case StmtType::INSERT: {
        InsertStmt *insert_stmt = static_cast<InsertStmt *>(stmt);
        rc                      = create_plan(insert_stmt, logical_operator);
      } break;

      case StmtType::DELETE: {
        DeleteStmt *delete_stmt = static_cast<DeleteStmt *>(stmt);
        rc                      = create_plan(delete_stmt, logical_operator);
      } break;

      case StmtType::EXPLAIN: {
        ExplainStmt *explain_stmt = static_cast<ExplainStmt *>(stmt);
        rc                        = create_plan(explain_stmt, logical_operator);
      } break;

      case StmtType::UPDATE: {
        UpdateStmt *update_stmt = static_cast<UpdateStmt *>(stmt);
        rc                      = create_plan(update_stmt, logical_operator);
      } break;
      default: {
        rc = RC::UNIMPLENMENT;
      }
    }
    return rc;
  }

private:
  static RC create_plan(CalcStmt *calc_stmt, std::unique_ptr<LogicalOperator> &logical_operator)
  {
    logical_operator.reset(new CalcLogicalOperator(std::move(calc_stmt->expressions())));
    return RC::SUCCESS;
  }

  
  static RC create_plan(SelectStmt* select_stmt, std::unique_ptr<LogicalOperator>& logical_operator) {
    RC rc = RC::SUCCESS;

    const std::vector<SelectStmt::JoinTables> &tables = select_stmt->join_tables();
    // const std::vector<Field> &all_fields = select_stmt->query_fields();

    auto process_one_table = [](std::unique_ptr<LogicalOperator> &prev_oper, Table *table, FilterStmt *fu) {
      std::vector<Field> fields;  
      std::unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_ONLY, fields));
      std::unique_ptr<LogicalOperator> predicate_oper;
      if (nullptr != fu) {
        if (RC rc = LogicalPlanGenerator::create_plan(fu, predicate_oper); rc != RC::SUCCESS) {
          LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
          return rc;
        }
      }
      if (prev_oper == nullptr) {
        // ASSERT(nullptr == fu, "ERROR!");
        if (predicate_oper) {
          static_cast<TableGetLogicalOperator *>(table_get_oper.get())
              ->set_predicates(std::move(predicate_oper->expressions()));
        }
        prev_oper = std::move(table_get_oper);
      } else {
        std::unique_ptr<JoinLogicalOperator> join_oper = std::make_unique<JoinLogicalOperator>();
        join_oper->add_child(std::move(prev_oper));
        join_oper->add_child(std::move(table_get_oper));
        if (predicate_oper) {
          predicate_oper->add_child(std::move(join_oper));
          prev_oper = std::move(predicate_oper);
        } else {
          prev_oper = std::move(join_oper);
        }
      }
      return RC::SUCCESS;
    };

    std::unique_ptr<LogicalOperator> outside_prev_oper(nullptr);  
    for (auto &jt : tables) {
      std::unique_ptr<LogicalOperator> prev_oper(nullptr);  // INNER JOIN
      auto                       &join_tables = jt.join_tables();
      auto                       &on_conds    = jt.on_conds();
      ASSERT(join_tables.size() == on_conds.size(), "ERROR!");
      for (size_t i = 0; i < join_tables.size(); ++i) {
        if (rc = process_one_table(prev_oper, join_tables[i], on_conds[i]); RC::SUCCESS != rc) {
          return rc;
        }
      }
      // now combine outside_prev_oper and prev_oper
      if (outside_prev_oper == nullptr) {
        outside_prev_oper = std::move(prev_oper);
      } else {
        std::unique_ptr<JoinLogicalOperator> join_oper = std::make_unique<JoinLogicalOperator>();
        join_oper->add_child(std::move(outside_prev_oper));
        join_oper->add_child(std::move(prev_oper));
        outside_prev_oper = std::move(join_oper);
      }
    }

    // set top oper
    ASSERT(outside_prev_oper, "ERROR!");                                  // TODO(wbj) Why doesn't work?
    std::unique_ptr<LogicalOperator> top_oper = std::move(outside_prev_oper);  // maybe null

    if (select_stmt->filter_stmt() != nullptr) {
      std::unique_ptr<LogicalOperator> predicate_oper;
      rc = create_plan(select_stmt->filter_stmt(), predicate_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
        return rc;
      }
      if (predicate_oper) {
        if (top_oper) {
          predicate_oper->add_child(std::move(top_oper));
        }
        top_oper = std::move(predicate_oper);
      }
    }
    if (select_stmt->groupby_stmt()) {
      

      auto                                &group_fields = select_stmt->groupby_stmt()->get_groupby_fields();
      std::vector<std::unique_ptr<OrderByUnit>> order_units;
      for (auto &expr : group_fields) {
        order_units.emplace_back(
            std::make_unique<OrderByUnit>(expr->deep_copy().release(), true)); 
      }


      std::vector<std::unique_ptr<Expression>> field_exprs;
      auto                                    &field = select_stmt->groupby_stmt()->get_field_exprs();
      for (auto &expr : field) {
        field_exprs.emplace_back(expr->deep_copy().release());
      }
      auto &tmp = select_stmt->groupby_stmt()->get_groupby_fields();
      for (auto &expr : tmp) {
        field_exprs.emplace_back(expr->deep_copy().release());
      }

      if (!select_stmt->groupby_stmt()->get_groupby_fields().empty()) {
        std::unique_ptr<LogicalOperator> orderby_oper(
            new OrderByLogicalOperator(std::move(order_units), std::move(field_exprs)));
        if (top_oper) {
          orderby_oper->add_child(std::move(top_oper));
        }
        top_oper = std::move(orderby_oper);
      }

      std::unique_ptr<LogicalOperator> groupby_oper;
      rc = create_plan(select_stmt->groupby_stmt(), groupby_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create groupby logical plan. rc=%s", strrc(rc));
        return rc;
      }

      groupby_oper->add_child(std::move(top_oper));
      top_oper = std::move(groupby_oper);

      // if(groupby_oper){
      //   if (top_oper) {
      //     groupby_oper->add_child(std::move(top_oper));
      //   }
      //   top_oper = std::move(groupby_oper);
      // }
    }

    if (select_stmt->having_stmt() != nullptr) {
      std::unique_ptr<LogicalOperator> predicate_oper;
      rc = create_plan(select_stmt->having_stmt(), predicate_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create having predicate logical plan. rc=%s", strrc(rc));
        return rc;
      }
      if (predicate_oper) {
        if (top_oper) {
          predicate_oper->add_child(std::move(top_oper));
        }
        top_oper = std::move(predicate_oper);
      }
    }

    if (select_stmt->orderby_stmt()) {
      std::unique_ptr<LogicalOperator> orderby_oper;
      rc = create_plan(select_stmt->orderby_stmt(), orderby_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create orderby logical plan. rc=%s", strrc(rc));
        return rc;
      }
      if (orderby_oper) {
        if (top_oper) {
          orderby_oper->add_child(std::move(top_oper));
        }
        top_oper = std::move(orderby_oper);
      }
    }
    {
      std::unique_ptr<LogicalOperator> project_oper(new ProjectLogicalOperator(std::move(select_stmt->projects())));
      ASSERT(project_oper,"ERROR!");
      if (top_oper) {
        project_oper->add_child(std::move(top_oper));
      }
      top_oper = std::move(project_oper);
    }

    logical_operator.swap(top_oper);
    return RC::SUCCESS;
  }

  static std::unique_ptr<PredicateLogicalOperator> cmp_exprs2predicate_logic_oper(std::vector<std::unique_ptr<Expression>> cmp_exprs)
  {
    if (!cmp_exprs.empty()) {
      std::unique_ptr<ConjunctionExpr> conjunction_expr(new ConjunctionExpr(ConjunctionExpr::Type::AND, cmp_exprs));
      return std::make_unique<PredicateLogicalOperator>(std::move(conjunction_expr));
    }
    return {};
  }

  static RC create_plan(FilterStmt* filter_stmt, std::unique_ptr<LogicalOperator>& logical_operator) {
    if (filter_stmt == nullptr || filter_stmt->condition() == nullptr) {
      return {};
    }
    std::vector<std::unique_ptr<Expression>> cmp_exprs;
 
    auto process_sub_query = [](Expression *expr) {
      if (expr->type() == ExprType::SUBQUERY) {
        SubQueryExpr *sub_query_expr = static_cast<SubQueryExpr *>(expr);
        return sub_query_expr->generate_logical_oper();
      }
      return RC::SUCCESS;
    };
    if (RC rc = filter_stmt->condition()->traverse_check(process_sub_query); OB_FAIL(rc)) {
      return rc;
    }
    cmp_exprs.emplace_back(std::move(filter_stmt->condition()));
    logical_operator = cmp_exprs2predicate_logic_oper(std::move(cmp_exprs));
    return RC::SUCCESS;
  }

  static RC create_plan(InsertStmt* insert_stmt, std::unique_ptr<LogicalOperator>& logical_operator) {
    Table                *table = insert_stmt->table();
    std::vector<std::vector<Value>> values;
    for (int i = 0; i < insert_stmt->values().size(); ++i) {
      values.emplace_back(insert_stmt->values()[i]);
    }

    InsertLogicalOperator *insert_operator = new InsertLogicalOperator(table, std::move(values));
    logical_operator.reset(insert_operator);
    return RC::SUCCESS;
  }

  static RC create_plan(DeleteStmt* delete_stmt, std::unique_ptr<LogicalOperator>& logical_operator) {
    Table             *table       = delete_stmt->table();
    FilterStmt        *filter_stmt = delete_stmt->filter_stmt();
    std::vector<Field> fields;
    for (int i = table->table_meta().sys_field_num(); i < table->table_meta().field_num(); i++) {
      const FieldMeta *field_meta = table->table_meta().field(i);
      fields.push_back(Field(table, field_meta));
    }
    std::unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_WRITE, fields));

    std::unique_ptr<LogicalOperator> predicate_oper;
    RC                          rc = create_plan(filter_stmt, predicate_oper);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    std::unique_ptr<LogicalOperator> delete_oper(new DeleteLogicalOperator(table));

    if (predicate_oper) {
      predicate_oper->add_child(std::move(table_get_oper));
      delete_oper->add_child(std::move(predicate_oper));
    } else {
      delete_oper->add_child(std::move(table_get_oper));
    }

    logical_operator = std::move(delete_oper);
    return rc;
  }
  static RC create_plan(ExplainStmt* explain_stmt, std::unique_ptr<LogicalOperator>& logical_operator) {
    Stmt                       *child_stmt = explain_stmt->child();
    std::unique_ptr<LogicalOperator> child_oper;
    RC                          rc = create(child_stmt, child_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create explain's child operator. rc=%s", strrc(rc));
      return rc;
    }

    logical_operator = std::unique_ptr<LogicalOperator>(new ExplainLogicalOperator);
    logical_operator->add_child(std::move(child_oper));
    return rc;
  }
  static RC create_plan(UpdateStmt* update_stmt, std::unique_ptr<LogicalOperator>& logical_operator) {
    Table             *table       = update_stmt->table();
    FilterStmt        *filter_stmt = update_stmt->filter_stmt();
    std::vector<Field> fields;
    for (int i = table->table_meta().sys_field_num(); i < table->table_meta().field_num(); i++) {
      const FieldMeta *field_meta = table->table_meta().field(i);
      fields.push_back(Field(table, field_meta));
    }
    std::unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_WRITE, fields));

    std::unique_ptr<LogicalOperator> predicate_oper;
    RC                          rc = RC::SUCCESS;
    if (filter_stmt != nullptr) {
      RC rc = create_plan(filter_stmt, predicate_oper);
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }
    auto process_sub_query = [](std::unique_ptr<Expression> &expr) {
      if (expr->type() == ExprType::SUBQUERY) {
        SubQueryExpr *sub_query_expr = static_cast<SubQueryExpr *>(expr.get());
        return sub_query_expr->generate_logical_oper();
      }
      return RC::SUCCESS;
    };
    for (auto &value : update_stmt->values()) {
      rc = process_sub_query(value);
      if (RC::SUCCESS != rc) {
        return rc;
      }
    }
    std::unique_ptr<LogicalOperator> update_oper(
        new UpdateLogicalOperator(table, std::move(update_stmt->values()), update_stmt->update_fields()));

    if (predicate_oper) {
      predicate_oper->add_child(std::move(table_get_oper));
      update_oper->add_child(std::move(predicate_oper));
    } else {
      update_oper->add_child(std::move(table_get_oper));
    }

    logical_operator = std::move(update_oper);
    return rc;
  }

  static RC create_plan(GroupByStmt* groupby_stmt, std::unique_ptr<LogicalOperator>& logical_operator) {
    if (groupby_stmt == nullptr) {
      logical_operator = nullptr;
      return RC::SUCCESS;
    }

    std::unique_ptr<LogicalOperator> groupby_oper(new GroupByLogicalOperator(std::move(groupby_stmt->get_groupby_fields()),
        std::move(groupby_stmt->get_agg_exprs()),
        std::move(groupby_stmt->get_field_exprs())));
    logical_operator = std::move(groupby_oper);
    return RC::SUCCESS;
  }

  static RC create_plan(OrderByStmt* orderby_stmt, std::unique_ptr<LogicalOperator>& logical_operator) {
    if (orderby_stmt == nullptr) {
      logical_operator = nullptr;
      return RC::SUCCESS;
    }

    std::unique_ptr<LogicalOperator> orderby_oper(new OrderByLogicalOperator(
        std::move(orderby_stmt->get_orderby_units()), std::move(orderby_stmt->get_exprs())));
    logical_operator = std::move(orderby_oper);
    return RC::SUCCESS;
  }
};