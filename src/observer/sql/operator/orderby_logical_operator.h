#pragma once

#include <vector>

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"
#include "sql/parser/value.h"
#include "storage/field/field.h"
#include "sql/stmt/orderby_stmt.h"
/**
 * @brief Âß¼­Ëã×Ó
 * @ingroup LogicalOperator
 */
class OrderByLogicalOperator : public LogicalOperator
{
public:
  OrderByLogicalOperator::OrderByLogicalOperator(
      std::vector<std::unique_ptr<OrderByUnit>> &&orderby_units, std::vector<std::unique_ptr<Expression>> &&exprs)
      : orderby_units_(std::move(orderby_units)), exprs_(std::move(exprs))
  {}
  virtual ~OrderByLogicalOperator() = default;

  LogicalOperatorType                        type() const override { return LogicalOperatorType::ORDER_BY; }
  std::vector<std::unique_ptr<OrderByUnit>> &orderby_units() { return orderby_units_; }
  std::vector<std::unique_ptr<Expression>>  &exprs() { return exprs_; }

private:
  std::vector<std::unique_ptr<OrderByUnit>> orderby_units_;  // ÅÅÐòÁÐ

  std::vector<std::unique_ptr<Expression>> exprs_;
};