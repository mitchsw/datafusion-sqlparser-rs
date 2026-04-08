// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::ast::Expr;
use crate::dialect::{Dialect, Precedence};
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

/// A [`Dialect`] for [ClickHouse](https://clickhouse.com/).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ClickHouseDialect {}

impl Dialect for ClickHouseDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://clickhouse.com/docs/en/sql-reference/syntax/#syntax-identifiers
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ch.is_ascii_digit()
    }

    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }

    fn supports_select_wildcard_except(&self) -> bool {
        true
    }

    fn describe_requires_table_keyword(&self) -> bool {
        true
    }

    fn require_interval_qualifier(&self) -> bool {
        true
    }

    fn supports_limit_comma(&self) -> bool {
        true
    }

    fn supports_insert_table_function(&self) -> bool {
        true
    }

    fn supports_insert_format(&self) -> bool {
        true
    }

    fn supports_numeric_literal_underscores(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/sql-reference/data-types/tuple#referring-to-tuple-elements>
    fn supports_tuple_index_syntax(&self) -> bool {
        true
    }

    // ClickHouse uses this for some FORMAT expressions in `INSERT` context, e.g. when inserting
    // with FORMAT JSONEachRow a raw JSON key-value expression is valid and expected.
    //
    // [ClickHouse formats](https://clickhouse.com/docs/en/interfaces/formats)
    fn supports_dictionary_syntax(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/functions#higher-order-functions---operator-and-lambdaparams-expr-function>
    fn supports_lambda_functions(&self) -> bool {
        true
    }

    fn supports_from_first_select(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by>
    fn supports_order_by_all(&self) -> bool {
        true
    }

    // See <https://clickhouse.com/docs/en/sql-reference/aggregate-functions/grouping_function#grouping-sets>
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/group-by#rollup-modifier>
    fn supports_group_by_with_modifier(&self) -> bool {
        true
    }

    /// Supported since 2020.
    /// See <https://clickhouse.com/docs/whats-new/changelog/2020#backward-incompatible-change-2>
    fn supports_nested_comments(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/optimize>
    fn supports_optimize_table(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/prewhere>
    fn supports_prewhere(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier>
    fn supports_with_fill(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/limit-by>
    fn supports_limit_by(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier>
    fn supports_interpolate(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select#settings-in-select-query>
    fn supports_settings(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/format>
    fn supports_select_format(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/sql-reference/statements/select#replace>
    fn supports_select_wildcard_replace(&self) -> bool {
        true
    }

    /// `FORMAT` and `SETTINGS` are query-level ClickHouse clauses parsed in
    /// `parse_query()` after `parse_select()` returns. Without this override
    /// they get consumed as implicit column aliases (e.g. `SELECT 1 FORMAT`
    /// becomes `SELECT 1 AS FORMAT`), preventing the downstream FORMAT/SETTINGS
    /// parsing from ever seeing the keyword.
    fn is_select_item_alias(&self, explicit: bool, kw: &Keyword, parser: &mut Parser) -> bool {
        if !explicit && matches!(kw, Keyword::FORMAT | Keyword::SETTINGS) {
            return false;
        }
        explicit || self.is_column_alias(kw, parser)
    }

    /// See <https://clickhouse.com/docs/sql-reference/operators#conditional-operator>
    fn supports_ternary_operator(&self) -> bool {
        true
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        let token = &parser.peek_token_ref().token;
        match token {
            // Bare `?` is the ternary operator; numbered placeholders like `?1` are not.
            Token::Placeholder(s) if s == "?" => {
                Some(Ok(self.prec_value(Precedence::Ternary)))
            }
            // ClickHouse does not use `:` as an infix operator (no Snowflake-style
            // variant access). Return unknown precedence so the Pratt loop leaves
            // it for the ternary handler to consume as a delimiter.
            Token::Colon => Some(Ok(self.prec_unknown())),
            _ => None,
        }
    }

    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &Expr,
        _precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        match &parser.peek_token_ref().token {
            Token::Placeholder(s) if s == "?" => {
                Some(self.parse_ternary_expr(parser, expr.clone()))
            }
            _ => None,
        }
    }
}

impl ClickHouseDialect {
    fn parse_ternary_expr(
        &self,
        parser: &mut Parser,
        condition: Expr,
    ) -> Result<Expr, ParserError> {
        // Consume the `?` token.
        parser.next_token();

        // The then-expression runs until the matching `:`. Because we
        // override `Token::Colon` to have unknown precedence for ClickHouse,
        // `parse_expr` will stop before consuming it.
        let if_true = parser.parse_expr()?;

        parser.expect_token(&Token::Colon)?;

        // Right-associative: use (ternary_prec - 1) so a subsequent `?`
        // at the same precedence level is consumed into this else-expression.
        let ternary_prec = self.prec_value(Precedence::Ternary);
        let if_false = parser.parse_subexpr(ternary_prec - 1)?;

        Ok(Expr::Ternary {
            condition: Box::new(condition),
            if_true: Box::new(if_true),
            if_false: Box::new(if_false),
        })
    }
}
