#pragma once

#include <vector>
#include <unordered_map>
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"
#include "sql/expr/expression.h"

class Db;
class Table;
class FieldMeta;


class FilterStmt
{
public:
  FilterStmt()          = default;
  virtual ~FilterStmt() = default;

public:
  std::unique_ptr<Expression> &condition() { return condition_; }

public:
  static RC create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      Expression *condition, FilterStmt *&stmt);

private:
  std::unique_ptr<Expression> condition_;
};