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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/groupby_stmt.h"
#include "sql/stmt/orderby_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"



SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
  if (nullptr != groupby_stmt_) {
    delete groupby_stmt_;
    groupby_stmt_ = nullptr;
  }
  if (nullptr != orderby_stmt_) {
    delete orderby_stmt_;
    orderby_stmt_ = nullptr;
  }
  if (nullptr != having_stmt_) {
    delete having_stmt_;
    having_stmt_ = nullptr;
  }
}

static void wildcard_fields(const Table *table, const std::string &alias,
    std::vector<std::unique_ptr<Expression>> &projects, bool is_single_table)
{
  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    auto field = table_meta.field(i);
    if (field->visible()) {
      FieldExpr *tmp = new FieldExpr(table, field);
      if (is_single_table) {
        tmp->set_name(tmp->get_field_name());  // should same as origin
      } else if (alias.empty()) {
        tmp->set_name(tmp->get_table_name() + "." + tmp->get_field_name());
      } else {
        tmp->set_name(alias + "." + tmp->get_field_name());
      }
      projects.emplace_back(tmp);
    }
  }
}

RC SelectStmt::process_from_clause(Db *db, std::vector<Table *> &tables,
    std::unordered_map<std::string, std::string> &table_alias_map, std::unordered_map<std::string, Table *> &table_map,
    std::vector<InnerJoinSqlNode> &from_relations, std::vector<JoinTables> &join_tables)
{
  std::unordered_set<std::string> table_alias_set;  // �������Ƿ����ظ�

  // collect tables info in `from` statement
  auto check_and_collect_tables = [&](const std::pair<std::string, std::string> &table_name_pair) {
    const std::string &src_name = table_name_pair.first;
    const std::string &alias    = table_name_pair.second;
    if (src_name.empty()) {
      LOG_WARN("invalid argument. relation name is null.");
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(src_name.c_str());
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), src_name.c_str());
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    tables.push_back(table);
    table_map.insert(std::pair<std::string, Table *>(src_name, table));
    if (!alias.empty()) {
      // ��Ҫ���Ǳ����ظ�������
      // NOTE: ���ﲻ���� table_map ��Ϊ������ parent table
      if (table_alias_set.count(alias) != 0) {
        return RC::INVALID_ARGUMENT;
      }
      table_alias_set.insert(alias);
      table_alias_map.insert(std::pair<std::string, std::string>(src_name, alias));
      table_map.insert(std::pair<std::string, Table *>(alias, table));
    }
    return RC::SUCCESS;
  };

  // t1 inner join t2 inner join t3 -> process t1 -> process t2 -> process t3
  auto process_one_relation =
      [&](const std::pair<std::string, std::string> &relation, JoinTables &jt, Expression *condition) {
        RC rc = RC::SUCCESS;
        // check and collect table to tables table_map local_table_map
        if (rc = check_and_collect_tables(relation); rc != RC::SUCCESS) {
          return rc;
        }

        // create filterstmt
        FilterStmt *filter_stmt = nullptr;
        if (condition != nullptr) {
          // TODO �������¿����¸��Ӳ�ѯ
          // TODO select * from t1 where c1 in (select * from t2 inner join t3 on t1.c1 > 0 inner join t1) ?
          if (rc = FilterStmt::create(db, table_map[relation.first], &table_map, condition, filter_stmt);
              rc != RC::SUCCESS) {
            return rc;
          }
          ASSERT(nullptr != filter_stmt, "FilterStmt is null!");
        }

        // fill JoinTables
        jt.push_join_table(table_map[relation.first], filter_stmt);
        return rc;
      };

  // xxx from (t1 inner join t2), (t3), (t4 inner join t5) where xxx
  for (size_t i = 0; i < from_relations.size(); ++i) {
    // t1 inner join t2 on xxx inner join t3 on xxx
    InnerJoinSqlNode &relations = from_relations[i];  // why not const : will clear its conditions
    // local_table_map = parent_table_map; // from clause �е� expr ����ʹ�ø���ѯ�ı��е��ֶ�

    // construct JoinTables
    JoinTables jt;

    // base relation: **t1** inner join t2 on xxx inner join t3 on xxx
    RC rc = process_one_relation(relations.base_relation, jt, nullptr);
    if (RC::SUCCESS != rc) {
      LOG_WARN("Create SelectStmt: From Clause: Process Base Relation %s Failed!", relations.base_relation.first.c_str());
      return rc;
    }

    // join relations: t1 inner join **t2** on xxx inner join **t3** on xxx
    const std::vector<std::pair<std::string, std::string>> &join_relations = relations.join_relations;
    std::vector<Expression *>                              &conditions     = relations.conditions;
    for (size_t j = 0; j < join_relations.size(); ++j) {
      if (RC::SUCCESS != (rc = process_one_relation(join_relations[j], jt, conditions[j]))) {
        return rc;
      }
    }
    conditions.clear();  // ������Ȩ�Ѿ��������� FilterStmt

    // push jt to join_tables
    join_tables.emplace_back(std::move(jt));
  }
  return RC::SUCCESS;
}

// parent_table_map �Ǹ���ѯ�е� table_map������ֻ��Ҫ����ѯ�� table map ����
// tables table_alias_map local_table_map ������Ҫ
// Ƕ���Ӳ�ѯ ��������� parent table map ���ۻ� ���ﲻ����ά���� vector
RC SelectStmt::create(
    Db *db, SelectSqlNode &select_sql, Stmt *&stmt, const std::unordered_map<std::string, Table *> &parent_table_map)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  std::vector<Table *>                         tables;           // �ռ����� table ��Ҫ���ڽ��� select *
  std::unordered_map<std::string, std::string> table_alias_map;  // <table src name, table alias name>
  std::unordered_map<std::string, Table *> table_map = parent_table_map;  // �ռ����� table �����������Ȳ�ѯ�� table
  std::vector<JoinTables> join_tables;
  RC rc = process_from_clause(db, tables, table_alias_map, table_map, select_sql.relations, join_tables);
  if (OB_FAIL(rc)) {
    LOG_WARN("SelectStmt-FromClause: Process Failed! RETURN %d", rc);
    return rc;
  }

  
  std::vector<std::unique_ptr<Expression>> projects;
  Table                                   *default_table   = nullptr;
  bool                                     is_single_table = (tables.size() == 1);
  if (is_single_table) {
    default_table = tables[0];
  }

  // ������е� FieldExpr �� SysFuncExpr �Ƿ�Ϸ� �����ж�һ���Ƿ��� AggrFuncExpr
  // projects �в������ subquery
  bool has_aggr_func_expr = false;
  auto check_project_expr = [&table_map, &tables, &default_table, &table_alias_map, &has_aggr_func_expr](
                                Expression *expr) {
    if (expr->type() == ExprType::SUBQUERY) {
      return RC::INVALID_ARGUMENT;
    }
    if (expr->type() == ExprType::SYSFUNCTION) {
      SysFuncExpr *sysfunc_expr = static_cast<SysFuncExpr *>(expr);
      return sysfunc_expr->check_param_type_and_number();
    }
    if (expr->type() == ExprType::FIELD) {
      FieldExpr *field_expr = static_cast<FieldExpr *>(expr);
      return field_expr->check_field(table_map, tables, default_table, table_alias_map);
    }
    if (expr->type() == ExprType::AGGRFUNCTION) {
      has_aggr_func_expr = true;
    }
    return RC::SUCCESS;
  };

  for (size_t i = 0; i < select_sql.expressions.size(); ++i) {
    Expression *expr = select_sql.project_exprs[i];
    // �������� select ��� * ����� select *; select *.*; select t1.*
    if (expr->type() == ExprType::FIELD) {
      FieldExpr  *field_expr = static_cast<FieldExpr *>(expr);
      const char *table_name = field_expr->get_table_name().c_str();
      const char *field_name = field_expr->get_field_name().c_str();
      ASSERT(!common::is_blank(field_name), "Parse ERROR!");
      if ((0 == strcmp(table_name, "*")) && (0 == strcmp(field_name, "*"))) {  // * or *.*
        if (tables.empty() || !field_expr->alias().empty()) {
          return RC::INVALID_ARGUMENT;  // not allow: select *; select * as xxx;
        }
        for (const Table *table : tables) {
          if (table_alias_map.count(table->name())) {
            wildcard_fields(table, table_alias_map[table->name()], projects, is_single_table);
          } else {
            wildcard_fields(table, std::string(), projects, is_single_table);
          }
        }
      } else if (0 == strcmp(field_name, "*")) {  // t1.*
        ASSERT(0 != strcmp(table_name, "*"), "Parse ERROR!");
        auto iter = table_map.find(table_name);
        if (iter == table_map.end()) {
          LOG_WARN("no such table in from list: %s", table_name);
          return RC::SCHEMA_FIELD_MISSING;
        }
        Table *table = iter->second;
        if (table_alias_map.count(table->name())) {
          wildcard_fields(table, table_alias_map[table->name()], projects, is_single_table);
        } else {
          wildcard_fields(table, std::string(), projects, is_single_table);
        }
      } else {  // t1.c1 or c1
        if (rc = field_expr->check_field(table_map, tables, default_table, table_alias_map); rc != RC::SUCCESS) {
          LOG_INFO("expr->check_field error!");
          return rc;
        }
        projects.emplace_back(expr);
      }
    } else {
      if (rc = expr->traverse_check(check_project_expr); rc != RC::SUCCESS) {
        LOG_WARN("project expr traverse check_field error!");
        return rc;
      }
      projects.emplace_back(expr);
    }
  }
  select_sql.project_exprs.clear();  // ����Ȩ�Ѿ��ƽ��� projects �� �����ύ�� select stmt

  LOG_INFO("got %d tables in from clause and %d exprs in query clause", tables.size(), projects.size());

  FilterStmt *filter_stmt = nullptr;  // TODO release memory when failed
  if (select_sql.conditions != nullptr) {
    rc = FilterStmt::create(db, default_table, &table_map, select_sql.conditions, filter_stmt);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot construct filter stmt");
      return rc;
    }
  }

  GroupByStmt *groupby_stmt       = nullptr;  // TODO release memory when failed
  FilterStmt  *having_filter_stmt = nullptr;  // TODO release memory when failed
  // �оۼ��������ʽ ������ group by clause ��Ҫ��� group by stmt
  if (has_aggr_func_expr || select_sql.groupby_exprs.size() > 0) {
    // 1. ��ȡ AggrFuncExpr �Լ����� AggrFuncExpr �е� FieldExpr
    std::vector<std::unique_ptr<AggrFuncExpr>> aggr_exprs;
    // select �Ӿ��г��ֵ����� fieldexpr ����Ҫ�����ռ�����,
    std::vector<std::unique_ptr<FieldExpr>>  field_exprs;           // ��� vector ��Ҫ���ݸ� order by ����
    std::vector<std::unique_ptr<Expression>> field_exprs_not_aggr;  // select ������з� aggrexpr ��
                                                                    // field_expr,�����ж�����Ƿ�Ϸ�
    // ���ڴ� project exprs ����ȡ���� aggr func exprs. e.g. min(c1 + 1) + 1
    auto collect_aggr_exprs = [&aggr_exprs](Expression *expr) {
      if (expr->type() == ExprType::AGGRFUNCTION) {
        aggr_exprs.emplace_back(static_cast<AggrFuncExpr *>(static_cast<AggrFuncExpr *>(expr)->deep_copy().release()));
      }
    };
    // ���ڴ� project exprs ����ȡ����field expr,
    auto collect_field_exprs = [&field_exprs](Expression *expr) {
      if (expr->type() == ExprType::FIELD) {
        field_exprs.emplace_back(static_cast<FieldExpr *>(static_cast<FieldExpr *>(expr)->deep_copy().release()));
      }
    };
    // ���ڴ� project exprs ����ȡ���в��� aggr func expr �е� field expr
    auto collect_exprs_not_aggexpr = [&field_exprs_not_aggr](Expression *expr) {
      if (expr->type() == ExprType::FIELD) {
        field_exprs_not_aggr.emplace_back(
            static_cast<FieldExpr *>(static_cast<FieldExpr *>(expr)->deep_copy().release()));
      }
    };
    // do extract
    for (auto &project : projects) {
      project->traverse(collect_aggr_exprs);   // ��ȡ���� aggexpr
      project->traverse(collect_field_exprs);  // ��ȡ select clause �е����� field_expr,���ݸ�groupby stmt
      // project->traverse(collect_field_exprs, [](const Expression* expr) { return expr->type() !=
      // ExprType::AGGRFUNCTION; });

      // ��ȡ���в��� aggexpr �е� field_expr������������
      project->traverse(
          collect_exprs_not_aggexpr, [](const Expression *expr) { return expr->type() != ExprType::AGGRFUNCTION; });
    }
    // ��� having ��ı��ʽ����Ҫ����������ͬ��������ȡ����
    //  select id, max(score) from t_group_by group by id having count(*)>5;
    if (select_sql.having_conditions != nullptr) {
      rc = FilterStmt::create(db, default_table, &table_map, select_sql.having_conditions, having_filter_stmt);
      if (rc != RC::SUCCESS) {
        LOG_WARN("cannot construct filter stmt");
        return rc;
      }
      // a. create filter stmt �� ��having �Ӿ��е��Ѿ����ݽ��� check_filed ��,���� ����� agg_expr������ȡ����
      auto &filter_expr = having_filter_stmt->condition();
      filter_expr->traverse(collect_aggr_exprs);   // ��ȡ���� aggexpr
      filter_expr->traverse(collect_field_exprs);  // ��ȡ select clause �е����� field_expr,���ݸ�groupby stmt
      // project->traverse(collect_field_exprs, [](const Expression* expr) { return expr->type() !=
      // ExprType::AGGRFUNCTION; }); ��ȡ���в��� aggexpr �е� field_expr������������
      filter_expr->traverse(
          collect_exprs_not_aggexpr, [](const Expression *expr) { return expr->type() != ExprType::AGGRFUNCTION; });
      select_sql.having_conditions = nullptr;
    }

    // 2. ������ check:
    // - �ۼ�������������������Ϊ * �ļ������ syntax parser ���
    // - �ۼ������е��ֶ� OK select clause ������

    // - û�� group by clause ʱ����Ӧ���зǾۼ������е��ֶ�
    if (!field_exprs_not_aggr.empty() && select_sql.groupby_exprs.size() == 0) {
      LOG_WARN("No Group By. But Has Fields Not In Aggr Func");
      return RC::INVALID_ARGUMENT;
    }

    // - �� group by��Ҫ�ж� select clause �� having clause �е� expr (�� agg_expr) ��һ�� group ��ֻ����һ��ֵ
    // e.g. select min(c1), c2+c3*c4 from t1 group by c2+c3, c4; YES
    //      select min(c1), c2, c3+c4 from t1 group by c2+c3;    NO
    if (select_sql.groupby_exprs.size() > 0) {
      // do check field
      for (size_t i = 0; i < select_sql.groupby_exprs.size(); i++) {
        Expression *expr = select_sql.groupby_exprs[i];
        if (rc = expr->traverse_check(check_project_expr); rc != RC::SUCCESS) {
          LOG_WARN("project expr traverse check_field error!");
          return rc;
        }
      }

      // ����ȡ select ��ķ� aggexpr ��Ȼ���ж����Ƿ��� groupby ��
      std::vector<Expression *> &groupby_exprs               = select_sql.groupby_exprs;
      auto                       check_expr_in_groupby_exprs = [&groupby_exprs](std::unique_ptr<Expression> &expr) {
        for (auto tmp : groupby_exprs) {
          if (expr->name() == tmp->name())  // ͨ�����ʽ�����ƽ����ж�
            return true;
        }
        return false;
      };

      // TODO û�м�� having �� order by �Ӿ��еı��ʽ
      for (auto &project : projects) {
        if (project->type() != ExprType::AGGRFUNCTION) {
          if (!check_expr_in_groupby_exprs(project)) {
            LOG_WARN("expr not in groupby_exprs");
            return RC::INVALID_ARGUMENT;
          }
        }
      }
    }

    // 3. create groupby stmt
    rc = GroupByStmt::create(db,
        default_table,
        &table_map,
        select_sql.groupby_exprs,
        groupby_stmt,
        std::move(aggr_exprs),
        std::move(field_exprs));
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot construct groupby stmt");
      return rc;
    }
    select_sql.groupby_exprs.clear();
    // 4. ������ƻ����ɽ׶��� groupby_operator �¹�һ�� orderby_operator
  }

  OrderByStmt *orderby_stmt = nullptr;  // TODO release memory when failed
  // create orderby stmt
  // ��Ϊ���� order by ��ʵ��Ҫ����������Ҫ������ ���Ի���Ҫ��ȡ TODO ������ܻ��ظ� �����Ȳ�����
  // - ����ȡ select clause ��� field_expr(��agg_expr�е�)���� agg_expr��������ȡʱ�Ѿ�����Ҫ�ٽ��� check �ˣ���Ϊ��
  // select clause
  // - order by ��� expr ���� check field
  if (select_sql.orderbys.size() > 0) {
    // ��ȡ AggrFuncExpr �Լ����� AggrFuncExpr �е� FieldExpr
    std::vector<std::unique_ptr<Expression>> expr_for_orderby;
    // ���ڴ� project exprs ����ȡ���� aggr func exprs. e.g. min(c1 + 1) + 1
    auto collect_aggr_exprs = [&expr_for_orderby](Expression *expr) {
      if (expr->type() == ExprType::AGGRFUNCTION) {
        expr_for_orderby.emplace_back(
            static_cast<AggrFuncExpr *>(static_cast<AggrFuncExpr *>(expr)->deep_copy().release()));
      }
    };
    // ���ڴ� project exprs ����ȡ���в��� aggr func expr �е� field expr
    auto collect_field_exprs = [&expr_for_orderby](Expression *expr) {
      if (expr->type() == ExprType::FIELD) {
        expr_for_orderby.emplace_back(static_cast<FieldExpr *>(static_cast<FieldExpr *>(expr)->deep_copy().release()));
      }
    };
    // do extract
    for (auto &project : projects) {
      project->traverse(collect_aggr_exprs);
      project->traverse(
          collect_field_exprs, [](const Expression *expr) { return expr->type() != ExprType::AGGRFUNCTION; });
    }
    // TODO ���Ӧ�÷ŵ� create ����ȥ���
    // do check field
    for (size_t i = 0; i < select_sql.orderbys.size(); i++) {
      Expression *expr = select_sql.orderbys[i].expr;
      if (rc = expr->traverse_check(check_project_expr); rc != RC::SUCCESS) {
        LOG_WARN("project expr traverse check_field error!");
        return rc;
      }
    }
    rc = OrderByStmt::create(
        db, default_table, &table_map, select_sql.orderbys, orderby_stmt, std::move(expr_for_orderby));
    if (RC::SUCCESS != rc) {
      return rc;
    }
    select_sql.orderbys.clear();
  }

  // everything alright
  // NOTE: ��ʱ select_sql ԭ�еĲ�����Ϣ�ѱ��Ƴ� ��������ʹ��
  SelectStmt *select_stmt = new SelectStmt();
  select_stmt->join_tables_.swap(join_tables);
  select_stmt->projects_.swap(projects);
  select_stmt->filter_stmt_  = filter_stmt;         // maybe nullptr
  select_stmt->groupby_stmt_ = groupby_stmt;        // maybe nullptr
  select_stmt->orderby_stmt_ = orderby_stmt;        // maybe nullptr
  select_stmt->having_stmt_  = having_filter_stmt;  // maybe nullptr
  stmt                       = select_stmt;
  return RC::SUCCESS;
}