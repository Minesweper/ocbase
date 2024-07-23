#pragma once

#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"

class DropIndexStmt : public Stmt
{
  StmtType type() const override { return StmtType::DROP_INDEX; }
};
