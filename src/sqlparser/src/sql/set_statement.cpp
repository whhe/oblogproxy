#include "set_statement.h"
// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {

SetStatement::SetStatement() : SQLStatement(COM_SET), sets(nullptr)
{}

SetStatement::~SetStatement()
{
  if (sets) {
    for (SetClause* set : *sets) {
      if (set != nullptr) {
        if (set->column != nullptr) {
          free(set->column);
        }
        if (set->value != nullptr) {
          delete set->value;
        }
        delete set;
      }
    }
    delete sets;
  }
}

SetPasswordStatement::SetPasswordStatement() : SQLStatement(COM_SET_PASSWORD), user(nullptr), password(nullptr)
{}

SetPasswordStatement::~SetPasswordStatement()
{
  if (user != nullptr) {
    free(user);
  }
  if (password != nullptr) {
    free(password);
  }
}
}  // namespace hsql
