#pragma once

#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"

class DropIndexStmt : public Stmt
{
public:
  StmtType type() const override { return StmtType::DROP_INDEX; }
  virtual ~DropIndexStmt() = default;
  DropIndexStmt(const std::string &table_name) : table_name_(table_name){};
  static RC          create(Db *db, const DropTableSqlNode &drop_table, Stmt *&stmt);
  const std::string &table_name() const { return table_name_; }
};
