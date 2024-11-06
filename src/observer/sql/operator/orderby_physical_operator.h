#pragma once

#include <memory>
#include "sql/operator/physical_operator.h"
#include "sql/expr/expression.h"
#include "sql/stmt/orderby_stmt.h"
#include "sql/expr/tuple.h"


class OrderByPhysicalOperator : public PhysicalOperator
{
public:
  OrderByPhysicalOperator(
      std::vector<std::unique_ptr<OrderByUnit>> &&orderby_units, std::vector<std::unique_ptr<Expression>> &&exprs);

  virtual ~OrderByPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::ORDER_BY; }

  RC fetch_and_sort_tables();
  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  std::vector<std::unique_ptr<OrderByUnit>> orderby_units_;  
  std::vector<std::vector<Value>>           values_;
  SplicedTuple                              tuple_;

  std::vector<int>           ordered_idx_;  
  std::vector<int>::iterator it_;
};