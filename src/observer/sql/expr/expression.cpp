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
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "sql/expr/arithmetic_operator.hpp"
#include "sql/stmt/select_stmt.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "sql/optimizer/logical_plan_generator.h"
#include "sql/optimizer/physical_plan_generator.h"
#include "common/lang/defer.h"

using namespace std;


std::string month_name[] = {"",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"};

RC FieldExpr::get_value(const Tuple &tuple, Value &value) 
{
  if (is_first) {
    bool &is_first_ref = const_cast<bool &>(is_first);
    is_first_ref       = false;
    return tuple.find_cell(TupleCellSpec(table_name(), field_name()), value, const_cast<int &>(index));
  } else {
    return tuple.cell_at(index, value);
  }
}

bool FieldExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::FIELD) {
    return false;
  }
  const auto &other_field_expr = static_cast<const FieldExpr &>(other);
  return table_name() == other_field_expr.table_name() && field_name() == other_field_expr.field_name();
}

RC FieldExpr::check_field(const std::unordered_map<std::string, Table *> &table_map, const std::vector<Table *> &tables,
    Table *default_table, const std::unordered_map<std::string, std::string> &table_alias_map)
{
  ASSERT(field_name_ != "*", "ERROR!");
  const char *table_name = table_name_.c_str();
  const char *field_name = field_name_.c_str();
  Table      *table      = nullptr;
  if (!common::is_blank(table_name)) {  // 表名不为空
    // check table
    auto iter = table_map.find(table_name);
    if (iter == table_map.end()) {
      LOG_WARN("no such table in from list: %s", table_name);
      return RC::SCHEMA_FIELD_MISSING;
    }
    table = iter->second;
  } else {  // 表名为空，只有列名
    if (tables.size() != 1 && default_table == nullptr) {
      LOG_WARN("invalid. I do not know the attr's table. attr=%s", this->get_field_name().c_str());
      return RC::SCHEMA_FIELD_MISSING;
    }
    table = default_table ? default_table : tables[0];
  }
  ASSERT(nullptr != table, "ERROR!");
  // set table_name
  table_name = table->name();
  // check field
  const FieldMeta *field_meta = table->table_meta().field(field_name);
  if (nullptr == field_meta) {
    LOG_WARN("no such field. field=%s.%s", table->name(), field_name);
    return RC::SCHEMA_FIELD_MISSING;
  }
  // set field_
  field_ = Field(table, field_meta);
  // set name 没用了 暂时保留它
  bool is_single_table = (tables.size() == 1);
  if (is_single_table) {
    set_name(field_name_);
  } else {
    set_name(table_name_ + "." + field_name_);
  }
  // set alias
  if (alias().empty()) {
    if (is_single_table) {
      set_alias(field_name_);
    } else {
      auto iter = table_alias_map.find(table_name_);
      if (iter != table_alias_map.end()) {
        set_alias(iter->second + "." + field_name_);
      } else {
        set_alias(table_name_ + "." + field_name_);
      }
    }
  }
  return RC::SUCCESS;
}

// TODO: 在进行表达式计算时，`chunk` 包含了所有列，因此可以通过 `field_id` 获取到对应列。
// 后续可以优化成在 `FieldExpr` 中存储 `chunk` 中某列的位置信息。
RC FieldExpr::get_column(Chunk &chunk, Column &column)
{
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    column.reference(chunk.column(field().meta()->field_id()));
  }
  return RC::SUCCESS;
}

bool ValueExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::VALUE) {
    return false;
  }
  const auto &other_value_expr = static_cast<const ValueExpr &>(other);
  return value_.compare(other_value_expr.get_value()) == 0;
}

RC ValueExpr::get_value(const Tuple &tuple, Value &value)
{
  value = value_;
  return RC::SUCCESS;
}

RC ValueExpr::get_column(Chunk &chunk, Column &column)
{
  column.init(value_);
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type) : child_(std::move(child)), cast_type_(cast_type)
{}

CastExpr::~CastExpr() {}

RC CastExpr::cast(const Value &value, Value &cast_value) const
{
  RC rc = RC::SUCCESS;
  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }

  switch (cast_type_) {
    case AttrType::BOOLEANS: {
      bool val = value.get_boolean();
      cast_value.set_boolean(val);
    } break;
    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported convert from type %d to %d", child_->value_type(), cast_type_);
    }
  }
  return rc;
}

RC CastExpr::get_value(const Tuple &tuple, Value &cell) 
{
  RC rc = child_->get_value(tuple, cell);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(cell, cell);
}

RC CastExpr::try_get_value(Value &value) const
{
  RC rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, value);
}

////////////////////////////////////////////////////////////////////////////////

ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : comp_(comp), left_(std::move(left)), right_(std::move(right))
{}

ComparisonExpr::ComparisonExpr(CompOp comp, Expression *left, Expression *right)
    : comp_(comp), left_(left), right_(right)
{}

ComparisonExpr::~ComparisonExpr() {}

RC ComparisonExpr::compare_value(const Value &left, const Value &right, bool &result) const
{
  RC  rc         = RC::SUCCESS;
  int cmp_result = left.compare(right);
  result         = false;
  switch (comp_) {
    case EQUAL_TO: {
      result = (0 == cmp_result);
    } break;
    case LESS_EQUAL: {
      result = (cmp_result <= 0);
    } break;
    case NOT_EQUAL: {
      result = (cmp_result != 0);
    } break;
    case LESS_THAN: {
      result = (cmp_result < 0);
    } break;
    case GREAT_EQUAL: {
      result = (cmp_result >= 0);
    } break;
    case GREAT_THAN: {
      result = (cmp_result > 0);
    } break;
    default: {
      LOG_WARN("unsupported comparison. %d", comp_);
      rc = RC::INTERNAL;
    } break;
  }

  return rc;
}

RC ComparisonExpr::try_get_value(Value &cell) const
{
  if (left_->type() == ExprType::VALUE && right_->type() == ExprType::VALUE) {
    ValueExpr *  left_value_expr  = static_cast<ValueExpr *>(left_.get());
    ValueExpr *  right_value_expr = static_cast<ValueExpr *>(right_.get());
    const Value &left_cell        = left_value_expr->get_value();
    const Value &right_cell       = right_value_expr->get_value();

    bool value = false;
    RC   rc    = compare_value(left_cell, right_cell, value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
    } else {
      cell.set_boolean(value);
    }
    return rc;
  }

  return RC::INVALID_ARGUMENT;
}

RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) 
{
  Value left_value;
  Value right_value;

  SubQueryExpr *left_subquery_expr  = nullptr;
  SubQueryExpr *right_subquery_expr = nullptr;

  DEFER(
    if (nullptr != left_subquery_expr) {
      left_subquery_expr->close();
    }
  );
  DEFER(
    if (nullptr != right_subquery_expr) {
      right_subquery_expr->close();
    }
  );

  auto if_subquery_open = [](const std::unique_ptr<Expression> &expr) {
    SubQueryExpr *sqexp = nullptr;
    if (expr->type() == ExprType::SUBQUERY) {
      sqexp = static_cast<SubQueryExpr *>(expr.get());
      sqexp->open(nullptr);  // 暂时先 nullptr
    }
    return sqexp;
  };
  left_subquery_expr  = if_subquery_open(left_);
  right_subquery_expr = if_subquery_open(right_);

  RC rc = RC::SUCCESS;
  if (comp_ == EXISTS_OP || comp_ == NOT_EXISTS_OP) {
    rc = right_->get_value(tuple, right_value);
    value.set_boolean(comp_ == EXISTS_OP ? rc == RC::SUCCESS : rc == RC::RECORD_EOF);
    return RC::RECORD_EOF == rc ? RC::SUCCESS : rc;
  }

  auto get_value = [&tuple](const std::unique_ptr<Expression> &expr, Value &value) {
    RC rc = expr->get_value(tuple, value);
    if (expr->type() == ExprType::SUBQUERY && RC::RECORD_EOF == rc) {
      value.set_null();
      rc = RC::SUCCESS;
    }
    return rc;
  };
  rc = get_value(left_, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  if (left_subquery_expr && left_subquery_expr->has_more_row(tuple)) {
    return RC::INVALID_ARGUMENT;
  }

  if (comp_ == IN_OP || comp_ == NOT_IN_OP) {
    if (left_value.is_null()) {
      value.set_boolean(false);
      return RC::SUCCESS;
    }
    if (right_->type() == ExprType::EXPRLIST) {
      static_cast<ExprListExpr *>(right_.get())->reset();
    }
    bool res      = false;  // 有一样的值
    bool has_null = false;  // 有一个 null
    while (RC::SUCCESS == (rc = right_->get_value(tuple, right_value))) {
      if (right_value.is_null()) {
        has_null = true;
      } else if (left_value.compare(right_value) == 0) {
        res = true;
      }
    }
    value.set_boolean(comp_ == IN_OP ? res : (has_null ? false : !res));
    return rc == RC::RECORD_EOF ? RC::SUCCESS : rc;
  }

  rc = get_value(right_, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  if (right_subquery_expr && right_subquery_expr->has_more_row(tuple)) {
    return RC::INVALID_ARGUMENT;
  }

  bool bool_value = false;
  rc              = compare_value(left_value, right_value, bool_value);
  if (rc == RC::SUCCESS) {
    value.set_boolean(bool_value);
  }
  return rc;
}

RC ComparisonExpr::eval(Chunk &chunk, std::vector<uint8_t> &select)
{
  RC     rc = RC::SUCCESS;
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  if (left_column.attr_type() != right_column.attr_type()) {
    LOG_WARN("cannot compare columns with different types");
    return RC::INTERNAL;
  }
  if (left_column.attr_type() == AttrType::INTS) {
    rc = compare_column<int>(left_column, right_column, select);
  } else if (left_column.attr_type() == AttrType::FLOATS) {
    rc = compare_column<float>(left_column, right_column, select);
  } else {
    // TODO: support string compare
    LOG_WARN("unsupported data type %d", left_column.attr_type());
    return RC::INTERNAL;
  }
  return rc;
}

template <typename T>
RC ComparisonExpr::compare_column(const Column &left, const Column &right, std::vector<uint8_t> &result) const
{
  RC rc = RC::SUCCESS;

  bool left_const  = left.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    compare_result<T, true, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else if (left_const && !right_const) {
    compare_result<T, true, false>((T *)left.data(), (T *)right.data(), right.count(), result, comp_);
  } else if (!left_const && right_const) {
    compare_result<T, false, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else {
    compare_result<T, false, false>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
ConjunctionExpr::ConjunctionExpr(ConjunctionExpr::Type type, vector<unique_ptr<Expression>> &children)
    : conjunction_type_(type), children_(std::move(children))
{}
ConjunctionExpr::ConjunctionExpr(ConjunctionExpr::Type type, Expression *left, Expression *right)
    : conjunction_type_(type)
{
  children_.emplace_back(left);
  children_.emplace_back(right);
}


RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) 
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(true);
    return rc;
  }

  Value tmp_value;
  for (const unique_ptr<Expression> &expr : children_) {
    rc = expr->get_value(tuple, tmp_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
      return rc;
    }
    bool bool_value = tmp_value.get_boolean();
    if ((conjunction_type_ == Type::AND && !bool_value) || (conjunction_type_ == Type::OR && bool_value)) {
      value.set_boolean(bool_value);
      return rc;
    }
  }

  bool default_value = (conjunction_type_ == Type::AND);
  value.set_boolean(default_value);
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

bool ArithmeticExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }
  auto &other_arith_expr = static_cast<const ArithmeticExpr &>(other);
  return arithmetic_type_ == other_arith_expr.arithmetic_type() && left_->equal(*other_arith_expr.left_) &&
         right_->equal(*other_arith_expr.right_);
}
AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if (left_->value_type() == AttrType::INTS && right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;
  }

  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();

  switch (arithmetic_type_) {
    case Type::ADD: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() + right_value.get_int());
      } else {
        value.set_float(left_value.get_float() + right_value.get_float());
      }
    } break;

    case Type::SUB: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() - right_value.get_int());
      } else {
        value.set_float(left_value.get_float() - right_value.get_float());
      }
    } break;

    case Type::MUL: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() * right_value.get_int());
      } else {
        value.set_float(left_value.get_float() * right_value.get_float());
      }
    } break;

    case Type::DIV: {
      if (target_type == AttrType::INTS) {
        if (right_value.get_int() == 0) {
          // NOTE:
          // 设置为整数最大值是不正确的。通常的做法是设置为NULL，但是当前的miniob没有NULL概念，所以这里设置为整数最大值。
          value.set_int(numeric_limits<int>::max());
        } else {
          value.set_int(left_value.get_int() / right_value.get_int());
        }
      } else {
        if (right_value.get_float() > -EPSILON && right_value.get_float() < EPSILON) {
          // NOTE:
          // 设置为浮点数最大值是不正确的。通常的做法是设置为NULL，但是当前的miniob没有NULL概念，所以这里设置为浮点数最大值。
          value.set_float(numeric_limits<float>::max());
        } else {
          value.set_float(left_value.get_float() / right_value.get_float());
        }
      }
    } break;

    case Type::NEGATIVE: {
      if (target_type == AttrType::INTS) {
        value.set_int(-left_value.get_int());
      } else {
        value.set_float(-left_value.get_float());
      }
    } break;

    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
    } break;
  }
  return rc;
}

template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
RC ArithmeticExpr::execute_calc(
    const Column &left, const Column &right, Column &result, Type type, AttrType attr_type) const
{
  RC rc = RC::SUCCESS;
  switch (type) {
    case Type::ADD: {
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, AddOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, AddOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLENMENT;
      }
    } break;
    case Type::SUB:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, SubtractOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, SubtractOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLENMENT;
      }
      break;
    case Type::MUL:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, MultiplyOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, MultiplyOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLENMENT;
      }
      break;
    case Type::DIV:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, DivideOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, DivideOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLENMENT;
      }
      break;
    case Type::NEGATIVE:
      if (attr_type == AttrType::INTS) {
        unary_operator<LEFT_CONSTANT, int, NegateOperator>((int *)left.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        unary_operator<LEFT_CONSTANT, float, NegateOperator>(
            (float *)left.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLENMENT;
      }
      break;
    default: rc = RC::UNIMPLENMENT; break;
  }
  if (rc == RC::SUCCESS) {
    result.set_count(result.capacity());
  }
  return rc;
}

RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) 
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_value(left_value, right_value, value);
}

RC ArithmeticExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
    return rc;
  }
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_column(left_column, right_column, column);
}

RC ArithmeticExpr::calc_column(const Column &left_column, const Column &right_column, Column &column) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  column.init(target_type, left_column.attr_len(), std::max(left_column.count(), right_column.count()));
  bool left_const  = left_column.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right_column.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    column.set_column_type(Column::Type::CONSTANT_COLUMN);
    rc = execute_calc<true, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (left_const && !right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<true, false>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (!left_const && right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, false>(left_column, right_column, column, arithmetic_type_, target_type);
  }
  return rc;
}

RC ArithmeticExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}

////////////////////////////////////////////////////////////////////////////////

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, Expression *child)
    : aggregate_name_(aggregate_name), child_(child)
{}

////////////////////////////////////////////////////////////////////////////////

AggregateExpr::AggregateExpr(AggrFuncType type, Expression *param)
    : AggregateExpr(type, std::unique_ptr<Expression>(param))
{}

AggregateExpr::AggregateExpr(AggrFuncType type, unique_ptr<Expression> param)
    : aggregate_type_(type), param_(std::move(param))
{
  auto check_is_constexpr = [](const Expression *expr) -> RC {
    if (expr->type() == ExprType::FIELD) {
      return RC::INTERNAL;
    }
    return RC::SUCCESS;
  };
  if (RC::SUCCESS == param_->traverse_check(check_is_constexpr)) {
    is_constexpr = true;
  }
}

RC AggregateExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    rc = RC::INTERNAL;
  }
  return rc;
}

bool AggregateExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != type()) {
    return false;
  }
  const AggregateExpr &other_aggr_expr = static_cast<const AggregateExpr &>(other);
  return aggregate_type_ == other_aggr_expr.aggregate_type() && param_->equal(*other_aggr_expr.child());
}

unique_ptr<Aggregator> AggregateExpr::create_aggregator() const
{
  unique_ptr<Aggregator> aggregator;
  switch (aggregate_type_) {
    case AggrFuncType::SUM: {
      aggregator = make_unique<SumAggregator>();
      break;
    }
    default: {
      ASSERT(false, "unsupported aggregate type");
      break;
    }
  }
  return aggregator;
}

RC AggregateExpr::get_value(const Tuple &tuple, Value &value) 
{
  TupleCellSpec spec(name());

  if (is_first) {
    bool &is_first_ref = const_cast<bool &>(is_first);
    is_first_ref       = false;
    return tuple.find_cell(spec, value, const_cast<int &>(index));
  } else {
    return tuple.cell_at(index, value);
  }
}

RC AggregateExpr::type_from_string(const char *type_str, AggrFuncType &type)
{
  RC rc = RC::SUCCESS;
  if (0 == strcasecmp(type_str, "count")) {
    type = AggrFuncType::COUNT;
  } else if (0 == strcasecmp(type_str, "sum")) {
    type = AggrFuncType::SUM;
  } else if (0 == strcasecmp(type_str, "avg")) {
    type = AggrFuncType::AVG;
  } else if (0 == strcasecmp(type_str, "max")) {
    type = AggrFuncType::MAX;
  } else if (0 == strcasecmp(type_str, "min")) {
    type = AggrFuncType::MIN;
  } else {
    rc = RC::INVALID_ARGUMENT;
  }
  return rc;
}

std::string AggregateExpr::get_func_name() const
{
  switch (aggregate_type_) {
    case AggrFuncType::MAX: return "max";
    case AggrFuncType::MIN: return "min";
    case AggrFuncType::SUM: return "sum";
    case AggrFuncType::AVG: return "avg";
    case AggrFuncType::COUNT: return "count";
    default: break;
  }
  return "unknown_aggr_fun";
}


SubQueryExpr::SubQueryExpr(const SelectSqlNode &sql_node) { sql_node_ = std::make_unique<SelectSqlNode>(sql_node); }

SubQueryExpr::~SubQueryExpr() = default;

RC SubQueryExpr::generate_select_stmt(Db *db, const std::unordered_map<std::string, Table *> &tables)
{
  Stmt *select_stmt = nullptr;
  if (RC rc = SelectStmt::create(db, *sql_node_.get(), select_stmt, tables); OB_FAIL(rc)) {
    return rc;
  }
  if (select_stmt->type() != StmtType::SELECT) {
    return RC::INVALID_ARGUMENT;
  }
  SelectStmt *ss = static_cast<SelectStmt *>(select_stmt);
  if (ss->projects().size() > 1) {
    return RC::INVALID_ARGUMENT;
  }
  stmt_ = std::unique_ptr<SelectStmt>(ss);
  return RC::SUCCESS;
}
RC SubQueryExpr::generate_logical_oper()
{
  if (RC rc = LogicalPlanGenerator::create(stmt_.get(), logical_oper_); OB_FAIL(rc)) {
    return rc;
  }
  return RC::SUCCESS;
}
RC SubQueryExpr::generate_physical_oper()
{
  if (RC rc = PhysicalPlanGenerator::create(*logical_oper_, physical_oper_); OB_FAIL(rc)) {
    LOG_WARN("subquery physical oper generate failed. return %s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}
// 子算子树的 open 和 close 逻辑由外部控制
RC SubQueryExpr::open(Trx *trx) { return physical_oper_->open(trx); }

RC SubQueryExpr::close() { return physical_oper_->close(); }

bool SubQueryExpr::has_more_row(const Tuple &tuple) const
{
  // TODO(wbj) 这里没考虑其他错误
  physical_oper_->set_parent_tuple(&tuple);
  return physical_oper_->next() != RC::RECORD_EOF;
}

RC SubQueryExpr::get_value(const Tuple &tuple, Value &value) 
{
  physical_oper_->set_parent_tuple(&tuple);
  // 每次返回一行的第一个 cell
  if (RC rc = physical_oper_->next(); RC::SUCCESS != rc) {
    return rc;
  }
  return physical_oper_->current_tuple()->cell_at(0, value);
}

RC SubQueryExpr::try_get_value(Value &value) const { return RC::UNIMPLENMENT; }

ExprType SubQueryExpr::type() const { return ExprType::SUBQUERY; }

AttrType SubQueryExpr::value_type() const { return UNDEFINED; }

std::unique_ptr<Expression> SubQueryExpr::deep_copy() const { return {}; }

RC SysFuncExpr::get_func_length_value(const Tuple &tuple, Value &value) const
{
  auto &param_expr = params_.front();
  Value param_cell;
  param_expr->get_value(tuple, param_cell);
  // unsupported not chars
  if (param_cell.attr_type() != CHARS) {
    return RC::INTERNAL;
  }
  int result_length = strlen(param_cell.data());
  value.set_int(result_length);
  return RC::SUCCESS;
}

RC SysFuncExpr::get_func_round_value(const Tuple &tuple, Value &value) const
{
  if (params_.size() > 1) {
    auto &param_expr           = params_[0];
    auto &param_expr_precision = params_[1];
    Value param_expr_cell;
    Value param_expr_precision_cell;
    param_expr->get_value(tuple, param_expr_cell);
    param_expr_precision->get_value(tuple, param_expr_precision_cell);
    if (param_expr_cell.attr_type() != FLOATS && param_expr_cell.attr_type() != DOUBLES) {
      return RC::INTERNAL;
    }
    if (param_expr_precision_cell.attr_type() != INTS) {
      return RC::INTERNAL;
    }
    float cell_float     = param_expr_cell.get_float();
    int   cell_precision = param_expr_precision_cell.get_int();
    auto  inner_round    = [](double d, int precision) {
      // std::cout << "Before: " << std::setprecision(12) << f << std::endl;
      std::stringstream ss;
      ss << std::fixed << std::setprecision(precision) << d;
      ss >> d;
      // std::cout << "After: " << std::setprecision(12) << f << std::endl;
      return d;
    };
    *(uint32_t *)&cell_float += 1;
    cell_float = inner_round(cell_float, cell_precision);
    // std::cout << cell_float << std::endl;
    value.set_float(cell_float);
  } else {
    auto &param_expr = *params_.begin();
    Value param_expr_cell;
    param_expr->get_value(tuple, param_expr_cell);
    if (param_expr_cell.attr_type() != FLOATS && param_expr_cell.attr_type() != DOUBLES) {
      return RC::INTERNAL;
    }
    float             cell_float = param_expr_cell.get_float();
    std::stringstream ss;
    ss << std::fixed << std::setprecision(0) << cell_float;
    ss >> cell_float;
    value.set_float(cell_float);
  }
  return RC::SUCCESS;
}
RC SysFuncExpr::get_func_data_format_value(const Tuple &tuple, Value &value) const
{
  auto &date_expr   = params_[0];
  auto &format_expr = params_[1];
  Value date_expr_cell;
  Value format_expr_cell;
  date_expr->get_value(tuple, date_expr_cell);
  format_expr->get_value(tuple, format_expr_cell);
  if (date_expr_cell.attr_type() != DATES) {
    return RC::INTERNAL;
  }
  if (format_expr_cell.attr_type() != CHARS) {
    return RC::INTERNAL;
  }
  int         cell_date         = date_expr_cell.get_int();
  const char *cell_format_chars = format_expr_cell.data();

  std::string result_date_str;
  int         year  = cell_date / 10000;
  int         month = (cell_date / 100) % 100;
  int         day   = cell_date % 100;
  for (size_t i = 0; i < strlen(cell_format_chars); i++) {
    // A ~ z
    if (65 <= cell_format_chars[i] && cell_format_chars[i] <= 122) {
      switch (cell_format_chars[i]) {
        case 'Y': {
          char tmp[5];
          sprintf(tmp, "%d", year);
          result_date_str += tmp;
          break;
        }
        case 'y': {
          char tmp[5];
          sprintf(tmp, "%d", year % 100);
          if (0 <= (year % 100) && (year % 100) <= 9) {
            result_date_str += "0";
          }
          result_date_str += tmp;
          break;
        }
        case 'M': {
          if (month <= 0 || month > 12) {
            return RC::INTERNAL;
          }
          result_date_str += month_name[month];
          break;
        }
        case 'm': {
          char tmp[3];
          sprintf(tmp, "%d", month);
          if (0 <= month && month <= 9) {
            result_date_str += "0";
          }
          result_date_str += tmp;
          break;
        }
        case 'D': {
          char tmp[3];
          sprintf(tmp, "%d", day);
          result_date_str += tmp;
          if (11 <= day && day <= 13) {
            result_date_str += "th";
          } else {
            switch (day % 10) {
              case 1: {
                result_date_str += "st";
                break;
              }
              case 2: {
                result_date_str += "nd";
                break;
              }
              case 3: {
                result_date_str += "rd";
                break;
              }
              default: {
                result_date_str += "th";
                break;
              }
            }
          }
          break;
        }
        case 'd': {
          char tmp[3];
          sprintf(tmp, "%d", day);
          if (0 <= day && day <= 9) {
            result_date_str += "0";
          }
          result_date_str += tmp;
          break;
        }
        default: {
          result_date_str += cell_format_chars[i];
          break;
        }
      }
    } else if (cell_format_chars[i] != '%') {
      result_date_str += cell_format_chars[i];
    }
  }
  value.set_string(result_date_str.c_str());
  return RC::SUCCESS;
}

RC SysFuncExpr::check_param_type_and_number() const
{
  RC rc = RC::SUCCESS;
  switch (func_type_) {
    case SYS_FUNC_LENGTH: {
      if (params_.size() != 1 || params_[0]->value_type() != CHARS)
        rc = RC::INVALID_ARGUMENT;
    } break;
    case SYS_FUNC_ROUND: {
      // 参数可以为一个或两个,第一个参数的结果类型 必须为 floats 或 double
      if ((params_.size() != 1 && params_.size() != 2) ||
          (params_[0]->value_type() != FLOATS && params_[0]->value_type() != DOUBLES))
        rc = RC::INVALID_ARGUMENT;
      // 如果有第二个参数，则类型必须为 INT
      if (params_.size() == 2) {
        if (params_[1]->value_type() != INTS) {
          rc = RC::INVALID_ARGUMENT;
        }
      }
    } break;
    case SYS_FUNC_DATE_FORMAT: {
      if (params_.size() != 2 || params_[0]->value_type() != DATES || params_[1]->value_type() != CHARS)
        rc = RC::INVALID_ARGUMENT;
    } break;
    default: {
      rc = RC::INVALID_ARGUMENT;
    } break;
  }
  return rc;
}
