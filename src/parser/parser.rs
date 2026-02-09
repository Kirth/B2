use crate::lexer::token::{Token, TokenKind};
use crate::runtime::value::Value;
use super::ast::{CallArg, Expr, InterpPart, MatchArm, MatchArmKind, ParamSpec, Pattern, Program, RecordField, Span, Stmt};

pub struct Parser<'a> {
    tokens: Vec<Token>,
    current: usize,
    source: &'a str,
    filename: String,
}

impl<'a> Parser<'a> {
    pub fn new(tokens: Vec<Token>, source: &'a str, filename: String) -> Self {
        Parser { tokens, current: 0, source, filename }
    }

    pub fn parse_program(&mut self) -> Result<Program, String> {
        let mut statements = Vec::new();
        while !self.is_at_end() {
            if self.match_kind(&TokenKind::Semicolon) { continue; }
            statements.push(self.parse_stmt()?);
        }
        Ok(Program { statements })
    }

    fn parse_stmt(&mut self) -> Result<Stmt, String> {
        if self.match_kind(&TokenKind::Let) {
            let span = self.prev_span();
            // Destructuring if next tokens indicate a pattern
            if self.check(&TokenKind::LParen) || self.check(&TokenKind::Underscore) || self.check(&TokenKind::Identifier(String::new())) {
                // Peek for comma to distinguish simple let
                let is_destructure = self.lookahead_has_comma_before_equal();
                if is_destructure {
                    let pattern = self.parse_pattern_list()?;
                    self.consume(&TokenKind::Equal, "Expected '=' after pattern")?;
                    let expr = self.parse_expression()?;
                    return Ok(Stmt::LetDestructure { pattern, expr, span });
                }
            }
            let name = self.consume_identifier("Expected variable name after 'let'")?;
            self.consume(&TokenKind::Equal, "Expected '=' after variable name")?;
            let expr = self.parse_expression()?;
            return Ok(Stmt::Let { name, expr, span });
        }
        if self.match_kind(&TokenKind::Fn) {
            return self.parse_function_stmt();
        }
        if self.match_kind(&TokenKind::Type) {
            return self.parse_type_alias_stmt();
        }
        if self.match_kind(&TokenKind::Record) {
            return self.parse_record_stmt();
        }
        if self.match_kind(&TokenKind::Return) {
            let span = self.prev_span();
            if self.check(&TokenKind::Semicolon) {
                self.advance();
                return Ok(Stmt::Return { value: None, span });
            }
            // Support multi-return: return a, b, c
            let mut values = Vec::new();
            values.push(self.parse_expression()?);
            while self.match_kind(&TokenKind::Comma) {
                values.push(self.parse_expression()?);
            }
            let value = if values.len() == 1 {
                values.remove(0)
            } else {
                Expr::Tuple { items: values, span }
            };
            return Ok(Stmt::Return { value: Some(value), span });
        }
        if self.match_kind(&TokenKind::Yield) {
            let span = self.prev_span();
            if self.check(&TokenKind::Semicolon) {
                self.advance();
                return Ok(Stmt::Yield { value: None, span });
            }
            let value = self.parse_expression()?;
            return Ok(Stmt::Yield {
                value: Some(value),
                span,
            });
        }
        if self.match_kind(&TokenKind::Continue) {
            let span = self.prev_span();
            if self.check(&TokenKind::Semicolon) {
                self.advance();
            }
            return Ok(Stmt::Continue { span });
        }
        if self.match_kind(&TokenKind::Break) {
            let span = self.prev_span();
            if self.check(&TokenKind::Semicolon) {
                self.advance();
            }
            return Ok(Stmt::Break { span });
        }
        if self.match_kind(&TokenKind::Throw) {
            let span = self.prev_span();
            let value = self.parse_expression()?;
            if self.check(&TokenKind::Semicolon) {
                self.advance();
            }
            return Ok(Stmt::Throw { value, span });
        }
        if self.match_kind(&TokenKind::Defer) {
            let span = self.prev_span();
            let body = self.parse_block()?;
            return Ok(Stmt::Defer { body, span });
        }
        if self.match_kind(&TokenKind::If) {
            return self.parse_if_stmt();
        }
        if self.match_kind(&TokenKind::For) {
            return self.parse_for_stmt();
        }
        if self.match_kind(&TokenKind::While) {
            return self.parse_while_stmt();
        }
        if self.match_kind(&TokenKind::Try) {
            let span = self.prev_span();
            let try_block = self.parse_block()?;
            let mut catch_name = None;
            let mut catch_block = None;
            let mut finally_block = None;

            if self.match_kind(&TokenKind::Catch) {
                catch_name = Some(self.consume_identifier("Expected catch variable name")?);
                catch_block = Some(self.parse_block()?);
            }
            if self.match_kind(&TokenKind::Finally) {
                finally_block = Some(self.parse_block()?);
            }
            if catch_block.is_none() && finally_block.is_none() {
                return Err(self.error_at_current("Expected 'catch' or 'finally' after try block"));
            }
            return Ok(Stmt::Try {
                try_block,
                catch_name,
                catch_block,
                finally_block,
                span,
            });
        }
        if self.match_kind(&TokenKind::Parallel) {
            return self.parse_parallel_for_stmt();
        }
        if self.match_kind(&TokenKind::Log) {
            let span = self.prev_span();
            let expr = self.parse_expression()?;
            return Ok(Stmt::Invoke { name: "log".to_string(), expr, span });
        }
        if self.match_kind(&TokenKind::Echo) {
            let span = self.prev_span();
            let expr = self.parse_expression()?;
            return Ok(Stmt::Invoke { name: "echo".to_string(), expr, span });
        }

        // assignment or expression
        let saved = self.current;
        if let Ok(assignable) = self.try_parse_assignable() {
            if self.match_kind(&TokenKind::Equal) {
                let expr = self.parse_expression()?;
                let span = assignable.span();
                return match assignable {
                    Expr::Var { name, .. } => Ok(Stmt::Assign { name, expr, span }),
                    Expr::Index { object, index, .. } => Ok(Stmt::IndexAssign { target: *object, index: *index, expr, span }),
                    _ => Err(self.error_at_current("Invalid assignment target")),
                };
            }
        }
        self.current = saved;
        let expr = self.parse_expression()?;
        Ok(Stmt::Expr { expr: expr.clone(), span: expr.span() })
    }

    fn parse_function_stmt(&mut self) -> Result<Stmt, String> {
        let span = self.prev_span();
        let name = self.consume_identifier("Expected function name after 'fn'")?;
        self.consume(&TokenKind::LParen, "Expected '(' after function name")?;
        let params = self.parse_param_list()?;
        self.consume(&TokenKind::RParen, "Expected ')' after parameter list")?;
        let return_type = if self.match_kind(&TokenKind::Arrow) {
            Some(self.parse_type()?)
        } else {
            None
        };
        let body = self.parse_block()?;
        let is_generator = contains_yield(&body);
        Ok(Stmt::Function {
            name,
            params,
            return_type,
            body,
            span,
            is_generator,
        })
    }

    fn parse_type_alias_stmt(&mut self) -> Result<Stmt, String> {
        let span = self.prev_span();
        let name = self.consume_identifier("Expected type alias name after 'type'")?;
        self.consume(&TokenKind::Equal, "Expected '=' in type alias")?;
        let target = self.parse_type()?;
        Ok(Stmt::TypeAlias { name, target, span })
    }

    fn parse_record_stmt(&mut self) -> Result<Stmt, String> {
        let span = self.prev_span();
        let name = self.consume_identifier("Expected record name after 'record'")?;
        self.consume(&TokenKind::LBrace, "Expected '{' after record name")?;
        let mut fields = Vec::new();
        while !self.check(&TokenKind::RBrace) && !self.is_at_end() {
            let field_name = self.consume_identifier("Expected record field name")?;
            self.consume(&TokenKind::Colon, "Expected ':' after record field name")?;
            let ty = self.parse_type()?;
            let default = if self.match_kind(&TokenKind::Equal) {
                Some(self.parse_expression()?)
            } else {
                None
            };
            fields.push(RecordField {
                name: field_name,
                ty,
                default,
            });
            if self.match_kind(&TokenKind::Comma) || self.match_kind(&TokenKind::Semicolon) {
                continue;
            }
            if self.check(&TokenKind::RBrace) {
                break;
            }
            return Err(self.error_at_current("Expected ',' or '}' after record field"));
        }
        self.consume(&TokenKind::RBrace, "Expected '}' after record fields")?;
        Ok(Stmt::RecordDef { name, fields, span })
    }

    fn parse_if_stmt(&mut self) -> Result<Stmt, String> {
        let span = self.prev_span();
        let cond = self.parse_expression()?;
        let then_branch = self.parse_block()?;
        let else_branch = if self.match_kind(&TokenKind::Else) {
            if self.match_kind(&TokenKind::If) {
                vec![self.parse_if_stmt()?]
            } else {
                self.parse_block()?
            }
        } else {
            Vec::new()
        };
        Ok(Stmt::If { cond, then_branch, else_branch, span })
    }

    fn parse_for_stmt(&mut self) -> Result<Stmt, String> {
        let span = self.prev_span();
        let pattern = self.parse_pattern_list()?;
        let in_tok = self.consume_identifier("Expected 'in' after loop variable")?;
        if in_tok != "in" {
            return Err(self.error_at_current("Expected 'in' after loop variable"));
        }
        let iterable = self.parse_expression()?;
        let body = self.parse_block()?;
        Ok(Stmt::For { pattern, iterable, body, span })
    }

    fn parse_parallel_for_stmt(&mut self) -> Result<Stmt, String> {
        let span = self.prev_span();
        self.consume(&TokenKind::For, "Expected 'for' after 'parallel'")?;
        let pattern = self.parse_pattern_list()?;
        let in_tok = self.consume_identifier("Expected 'in' after loop variable")?;
        if in_tok != "in" {
            return Err(self.error_at_current("Expected 'in' after loop variable"));
        }
        let iterable = self.parse_expression()?;
        let body = self.parse_block()?;
        Ok(Stmt::ParallelFor { pattern, iterable, body, span })
    }

    fn parse_parallel_for_expr(&mut self) -> Result<Expr, String> {
        let span = self.prev_span();
        self.consume(&TokenKind::For, "Expected 'for' after 'parallel'")?;
        let pattern = self.parse_pattern_list()?;
        let in_tok = self.consume_identifier("Expected 'in' after loop variable")?;
        if in_tok != "in" {
            return Err(self.error_at_current("Expected 'in' after loop variable"));
        }
        let iterable = self.parse_expression()?;
        let body = self.parse_block()?;
        Ok(Expr::ParallelFor { pattern, iterable: Box::new(iterable), body, span })
    }

    fn parse_while_stmt(&mut self) -> Result<Stmt, String> {
        let span = self.prev_span();
        let cond = self.parse_expression()?;
        let body = self.parse_block()?;
        Ok(Stmt::While { cond, body, span })
    }

    fn parse_expression(&mut self) -> Result<Expr, String> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_and()?;
        while self.match_kind(&TokenKind::OrOr) {
            let op = self.previous_lexeme();
            let right = self.parse_and()?;
            let span = expr.span();
            expr = Expr::Binary { left: Box::new(expr), op, right: Box::new(right), span };
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_equality()?;
        while self.match_kind(&TokenKind::AndAnd) {
            let op = self.previous_lexeme();
            let right = self.parse_equality()?;
            let span = expr.span();
            expr = Expr::Binary { left: Box::new(expr), op, right: Box::new(right), span };
        }
        Ok(expr)
    }

    fn parse_equality(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_comparison()?;
        while self.match_any(&[TokenKind::EqualEqual, TokenKind::NotEqual]) {
            let op = self.previous_lexeme();
            let right = self.parse_comparison()?;
            let span = expr.span();
            expr = Expr::Binary { left: Box::new(expr), op, right: Box::new(right), span };
        }
        Ok(expr)
    }

    fn parse_comparison(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_term()?;
        while self.match_any(&[TokenKind::Less, TokenKind::LessEqual, TokenKind::Greater, TokenKind::GreaterEqual]) {
            let op = self.previous_lexeme();
            let right = self.parse_term()?;
            let span = expr.span();
            expr = Expr::Binary { left: Box::new(expr), op, right: Box::new(right), span };
        }
        Ok(expr)
    }

    fn parse_term(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_factor()?;
        while self.match_any(&[TokenKind::Plus, TokenKind::Minus]) {
            let op = self.previous_lexeme();
            let right = self.parse_factor()?;
            let span = expr.span();
            expr = Expr::Binary { left: Box::new(expr), op, right: Box::new(right), span };
        }
        Ok(expr)
    }

    fn parse_factor(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_unary()?;
        while self.match_any(&[TokenKind::Star, TokenKind::Slash, TokenKind::Percent]) {
            let op = self.previous_lexeme();
            let right = self.parse_unary()?;
            let span = expr.span();
            expr = Expr::Binary { left: Box::new(expr), op, right: Box::new(right), span };
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<Expr, String> {
        if self.match_kind(&TokenKind::Await) {
            let span = self.prev_span();
            let safe = self.match_kind(&TokenKind::Question);
            let expr = self.parse_unary()?;
            return Ok(Expr::Await { expr: Box::new(expr), safe, span });
        }
        if self.match_any(&[TokenKind::Bang, TokenKind::Minus]) {
            let op = self.previous_lexeme();
            let span = self.prev_span();
            let expr = self.parse_unary()?;
            return Ok(Expr::Unary { op, expr: Box::new(expr), span });
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.is_record_constructor_start() {
                if let Expr::Var { .. } = expr {
                    let span = expr.span();
                    let args = self.parse_brace_named_args()?;
                    expr = Expr::Call {
                        callee: Box::new(expr),
                        args,
                        span,
                    };
                    continue;
                }
            }
            if self.match_kind(&TokenKind::Dot) {
                let span = self.prev_span();
                let name = self.consume_identifier("Expected member name after '.'")?;
                expr = Expr::Member { object: Box::new(expr), name, span };
                continue;
            }
            if self.match_kind(&TokenKind::LParen) {
                let span = self.prev_span();
                let args = self.parse_arguments()?;
                self.consume(&TokenKind::RParen, "Expected ')' after arguments")?;
                expr = Expr::Call { callee: Box::new(expr), args, span };
                continue;
            }
            if self.match_kind(&TokenKind::LBracket) {
                let span = self.prev_span();
                let index = self.parse_expression()?;
                self.consume(&TokenKind::RBracket, "Expected ']' after index")?;
                expr = Expr::Index { object: Box::new(expr), index: Box::new(index), span };
                continue;
            }
            if self.match_kind(&TokenKind::DotDot) {
                let span = self.prev_span();
                let end = self.parse_expression()?;
                expr = Expr::Range { start: Box::new(expr), end: Box::new(end), span };
                continue;
            }
            break;
        }
        Ok(expr)
    }

    fn parse_brace_named_args(&mut self) -> Result<Vec<CallArg>, String> {
        self.consume(&TokenKind::LBrace, "Expected '{' after type name")?;
        let mut args = Vec::new();
        while !self.check(&TokenKind::RBrace) && !self.is_at_end() {
            let name = self.consume_identifier("Expected field name in record constructor")?;
            self.consume(&TokenKind::Equal, "Expected '=' after field name")?;
            let value = self.parse_expression()?;
            args.push(CallArg::Named { name, value });
            if self.match_kind(&TokenKind::Comma) {
                continue;
            }
            if self.check(&TokenKind::RBrace) {
                break;
            }
            return Err(self.error_at_current("Expected ',' or '}' in record constructor"));
        }
        self.consume(&TokenKind::RBrace, "Expected '}' after record constructor fields")?;
        Ok(args)
    }

    fn is_record_constructor_start(&self) -> bool {
        if !self.check(&TokenKind::LBrace) {
            return false;
        }
        if self.current + 1 >= self.tokens.len() {
            return false;
        }
        // Allow empty constructor braces: TypeName {}
        if matches!(self.tokens[self.current + 1].kind, TokenKind::RBrace) {
            return true;
        }
        if self.current + 2 >= self.tokens.len() {
            return false;
        }
        matches!(self.tokens[self.current + 1].kind, TokenKind::Identifier(_))
            && matches!(self.tokens[self.current + 2].kind, TokenKind::Equal)
    }

    fn parse_primary(&mut self) -> Result<Expr, String> {
        if self.match_kind(&TokenKind::True) {
            let span = self.prev_span();
            return Ok(Expr::Literal { value: Value::Bool(true), span });
        }
        if self.match_kind(&TokenKind::False) {
            let span = self.prev_span();
            return Ok(Expr::Literal { value: Value::Bool(false), span });
        }
        if self.match_kind(&TokenKind::Number(String::new())) {
            if let TokenKind::Number(text) = self.previous().kind.clone() {
                let span = self.prev_span();
                let val: f64 = text.parse().map_err(|_| self.error_at_current("Invalid number"))?;
                return Ok(Expr::Literal { value: Value::Number(val), span });
            }
        }
        if self.match_kind(&TokenKind::TimeLiteral(String::new())) {
            if let TokenKind::TimeLiteral(text) = self.previous().kind.clone() {
                let span = self.prev_span();
                return Ok(Expr::Literal { value: Value::Duration(Value::parse_duration(&text)?), span });
            }
        }
        if self.match_kind(&TokenKind::String(String::new())) {
            if let TokenKind::String(text) = self.previous().kind.clone() {
                let span = self.prev_span();
                return Ok(Expr::Literal { value: Value::String(text), span });
            }
        }
        if self.match_kind(&TokenKind::InterpolatedString(String::new())) {
            if let TokenKind::InterpolatedString(text) = self.previous().kind.clone() {
                let parts = self.parse_interpolated_string(&text)?;
                let span = self.prev_span();
                return Ok(Expr::InterpolatedString { parts, span });
            }
        }
        if self.check(&TokenKind::Identifier(String::new())) && self.peek_next_is_arrow() {
            return self.parse_arrow_lambda_single();
        }
        if self.check(&TokenKind::LParen) && self.next_is_arrow_after_params() {
            return self.parse_arrow_lambda_multi();
        }
        if self.match_kind(&TokenKind::Identifier(String::new())) {
            if let TokenKind::Identifier(name) = self.previous().kind.clone() {
                let span = self.prev_span();
                return Ok(Expr::Var { name, span });
            }
        }
        if self.match_kind(&TokenKind::If) {
            let span = self.prev_span();
            let cond = self.parse_expression()?;
            let then_branch = self.parse_block()?;
            let else_branch = if self.match_kind(&TokenKind::Else) { self.parse_block()? } else { Vec::new() };
            return Ok(Expr::IfExpr { cond: Box::new(cond), then_branch, else_branch, span });
        }
        if self.match_kind(&TokenKind::Match) {
            return self.parse_match_expr();
        }
        if self.match_kind(&TokenKind::Parallel) {
            return self.parse_parallel_for_expr();
        }
        if self.match_kind(&TokenKind::Task) {
            let span = self.prev_span();
            if self.check(&TokenKind::LBrace) {
                let body = self.parse_block()?;
                return Ok(Expr::TaskBlock { body, span });
            }
            if self.match_kind(&TokenKind::LParen) {
                let mut args = self.parse_arguments()?;
                self.consume(&TokenKind::RParen, "Expected ')' after task arguments")?;
                if args.is_empty() {
                    return Err(self.error_at_current("task(...) expects callable as first argument"));
                }
                let callee = match args.remove(0) {
                    CallArg::Positional(expr) => expr,
                    CallArg::Spread(_) => {
                        return Err(self.error_at_current("task(...) expects callable as first positional argument"));
                    }
                    CallArg::Named { .. } => {
                        return Err(self.error_at_current("task(...) expects callable as first positional argument"));
                    }
                };
                return Ok(Expr::TaskCall { callee: Box::new(callee), args, span });
            }
            return Err(self.error_at_current("Expected '{' or '(' after 'task'"));
        }
        if self.match_kind(&TokenKind::Fn) {
            let span = self.prev_span();
            self.consume(&TokenKind::LParen, "Expected '(' after 'fn'")?;
            let params = self.parse_param_list()?;
            self.consume(&TokenKind::RParen, "Expected ')' after parameters")?;
            let return_type = if self.match_kind(&TokenKind::Arrow) {
                Some(self.parse_type()?)
            } else {
                None
            };
            let body = self.parse_block()?;
            return Ok(Expr::Lambda { params, return_type, body, span });
        }
        if self.match_kind(&TokenKind::Sh) {
            let span = self.prev_span();
            let command = if self.check(&TokenKind::LBrace) {
                self.parse_shell_block()?
            } else {
                self.parse_expression()?
            };
            return Ok(Expr::Sh { command: Box::new(command), span });
        }
        if self.match_kind(&TokenKind::Ssh) {
            let span = self.prev_span();
            let host = self.parse_expression()?;
            let command = if self.check(&TokenKind::LBrace) {
                self.parse_shell_block()?
            } else {
                self.parse_expression()?
            };
            return Ok(Expr::Ssh { host: Box::new(host), command: Box::new(command), span });
        }
        if self.match_kind(&TokenKind::LParen) {
            let span = self.prev_span();
            let first = self.parse_expression()?;
            if self.match_kind(&TokenKind::Comma) {
                let mut items = vec![first];
                loop {
                    items.push(self.parse_expression()?);
                    if !self.match_kind(&TokenKind::Comma) {
                        break;
                    }
                }
                self.consume(&TokenKind::RParen, "Expected ')' after tuple")?;
                return Ok(Expr::Tuple { items, span });
            }
            self.consume(&TokenKind::RParen, "Expected ')' after expression")?;
            return Ok(first);
        }
        if self.match_kind(&TokenKind::LBracket) {
            let span = self.prev_span();
            if self.match_kind(&TokenKind::For) {
                return self.parse_array_comprehension(span);
            }
            let elements = self.parse_elements()?;
            self.consume(&TokenKind::RBracket, "Expected ']' after array")?;
            return Ok(Expr::Array { items: elements, span });
        }
        if self.match_kind(&TokenKind::LBrace) {
            let span = self.prev_span();
            if self.is_dict_start() {
                let pairs = self.parse_dict_entries()?;
                self.consume(&TokenKind::RBrace, "Expected '}' after dict")?;
                return Ok(Expr::Dict { items: pairs, span });
            }
            return Err(self.error_at_current("Unexpected '{'"));
        }

        Err(self.error_at_current("Unexpected token in expression"))
    }

    fn parse_match_expr(&mut self) -> Result<Expr, String> {
        let span = self.prev_span();
        let subject = self.parse_expression()?;
        self.consume(&TokenKind::LBrace, "Expected '{' after match subject")?;

        let mut arms = Vec::new();
        let mut wildcard_count = 0usize;

        while !self.check(&TokenKind::RBrace) && !self.is_at_end() {
            let kind = if self.match_kind(&TokenKind::Underscore) {
                wildcard_count += 1;
                MatchArmKind::Wildcard
            } else if self.match_any(&[
                TokenKind::Less,
                TokenKind::LessEqual,
                TokenKind::Greater,
                TokenKind::GreaterEqual,
                TokenKind::EqualEqual,
                TokenKind::NotEqual,
            ]) {
                let op = self.previous_lexeme();
                let rhs = self.parse_expression()?;
                MatchArmKind::Compare { op, rhs }
            } else {
                let value = self.parse_expression()?;
                MatchArmKind::Value(value)
            };

            self.consume(&TokenKind::FatArrow, "Expected '=>' in match arm")?;
            let body = self.parse_match_arm_body()?;
            arms.push(MatchArm { kind, body });

            if self.match_kind(&TokenKind::Comma) {
                continue;
            }
            if self.check(&TokenKind::RBrace) {
                break;
            }
            return Err(self.error_at_current("Expected ',' or '}' after match arm"));
        }

        self.consume(&TokenKind::RBrace, "Expected '}' after match")?;
        if wildcard_count == 0 {
            return Err(format!(
                "{}:{}:{}: match requires a wildcard arm '_'",
                self.filename, span.line, span.col
            ));
        }
        if wildcard_count > 1 {
            return Err(format!(
                "{}:{}:{}: match can only contain one wildcard arm '_'",
                self.filename, span.line, span.col
            ));
        }

        Ok(Expr::Match { subject: Box::new(subject), arms, span })
    }

    fn parse_array_comprehension(&mut self, span: Span) -> Result<Expr, String> {
        let pattern = self.parse_pattern_list()?;
        let in_tok = self.consume_identifier("Expected 'in' after loop variable")?;
        if in_tok != "in" {
            return Err(self.error_at_current("Expected 'in' after loop variable"));
        }
        let iterable = self.parse_expression()?;
        let guard = if self.match_kind(&TokenKind::If) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };
        self.consume(&TokenKind::FatArrow, "Expected '=>' in array comprehension")?;
        let map_expr = self.parse_expression()?;
        self.consume(&TokenKind::RBracket, "Expected ']' after array comprehension")?;
        Ok(Expr::ArrayComprehension {
            pattern,
            iterable: Box::new(iterable),
            guard,
            map_expr: Box::new(map_expr),
            span,
        })
    }

    fn parse_match_arm_body(&mut self) -> Result<Vec<Stmt>, String> {
        if self.check(&TokenKind::LBrace) {
            return self.parse_block();
        }
        let expr = self.parse_expression()?;
        Ok(vec![Stmt::Expr { expr: expr.clone(), span: expr.span() }])
    }

    fn parse_param_list(&mut self) -> Result<Vec<ParamSpec>, String> {
        let mut params = Vec::new();
        let mut saw_default = false;
        let mut saw_variadic = false;
        if !self.check(&TokenKind::RParen) {
            loop {
                let variadic = self.match_kind(&TokenKind::Ellipsis);
                if saw_variadic {
                    return Err(self.error_at_current("Variadic parameter must be the final parameter"));
                }
                let name = self.consume_identifier("Expected parameter name")?;
                let ty = if self.match_kind(&TokenKind::Colon) {
                    Some(self.parse_type()?)
                } else {
                    None
                };
                let default = if self.match_kind(&TokenKind::Equal) {
                    if variadic {
                        return Err(self.error_at_current("Variadic parameter cannot have a default value"));
                    }
                    saw_default = true;
                    Some(self.parse_expression()?)
                } else {
                    None
                };
                if variadic {
                    saw_variadic = true;
                }
                if saw_default && default.is_none() {
                    return Err(self.error_at_current("Required parameters cannot follow parameters with defaults"));
                }
                params.push(ParamSpec { name, ty, default, variadic });
                if !self.match_kind(&TokenKind::Comma) { break; }
            }
        }
        Ok(params)
    }

    fn parse_arguments(&mut self) -> Result<Vec<CallArg>, String> {
        let mut args = Vec::new();
        let mut saw_named = false;
        if !self.check(&TokenKind::RParen) {
            loop {
                if self.check(&TokenKind::Identifier(String::new())) && self.peek_next_is_equal() {
                    saw_named = true;
                    let name = self.consume_identifier("Expected argument name")?;
                    self.consume(&TokenKind::Equal, "Expected '=' in named argument")?;
                    let value = self.parse_expression()?;
                    args.push(CallArg::Named { name, value });
                } else {
                    if saw_named {
                        return Err(self.error_at_current("Positional arguments cannot appear after named arguments"));
                    }
                    if self.match_kind(&TokenKind::Ellipsis) {
                        let spread_expr = self.parse_expression()?;
                        args.push(CallArg::Spread(spread_expr));
                    } else {
                        args.push(CallArg::Positional(self.parse_expression()?));
                    }
                }
                if !self.match_kind(&TokenKind::Comma) { break; }
            }
        }
        Ok(args)
    }

    fn parse_elements(&mut self) -> Result<Vec<Expr>, String> {
        let mut elements = Vec::new();
        if !self.check(&TokenKind::RBracket) {
            loop {
                elements.push(self.parse_expression()?);
                if !self.match_kind(&TokenKind::Comma) { break; }
            }
        }
        Ok(elements)
    }

    fn parse_dict_entries(&mut self) -> Result<Vec<(Expr, Expr)>, String> {
        let mut pairs = Vec::new();
        if !self.check(&TokenKind::RBrace) {
            loop {
                let key = self.parse_dict_key()?;
                self.consume(&TokenKind::Colon, "Expected ':' in dict")?;
                let value = self.parse_expression()?;
                pairs.push((key, value));
                if !self.match_kind(&TokenKind::Comma) { break; }
            }
        }
        Ok(pairs)
    }

    fn parse_dict_key(&mut self) -> Result<Expr, String> {
        if self.check(&TokenKind::Identifier(String::new())) && matches!(self.peek_next().kind, TokenKind::Colon) {
            let key = self.consume_identifier("Expected dict key")?;
            let span = self.prev_span();
            return Ok(Expr::Literal { value: Value::String(key), span });
        }
        self.parse_expression()
    }

    fn is_dict_start(&self) -> bool {
        if self.check(&TokenKind::RBrace) { return true; }
        match &self.peek().kind {
            TokenKind::Identifier(_) | TokenKind::String(_) => {
                matches!(self.peek_next().kind, TokenKind::Colon)
            }
            _ => false,
        }
    }

    fn parse_block(&mut self) -> Result<Vec<Stmt>, String> {
        self.consume(&TokenKind::LBrace, "Expected '{'")?;
        let mut statements = Vec::new();
        while !self.check(&TokenKind::RBrace) && !self.is_at_end() {
            statements.push(self.parse_stmt()?);
        }
        self.consume(&TokenKind::RBrace, "Expected '}' after block")?;
        Ok(statements)
    }

    fn parse_shell_block(&mut self) -> Result<Expr, String> {
        let open = self.consume(&TokenKind::LBrace, "Expected '{' for shell block")?;
        let mut depth = 1usize;
        let mut pos = open.start + 1;
        let bytes = self.source.as_bytes();
        while pos < bytes.len() && depth > 0 {
            let c = bytes[pos] as char;
            if c == '{' { depth += 1; }
            else if c == '}' { depth -= 1; }
            pos += 1;
        }
        if depth != 0 {
            return Err(self.error_at_current("Unbalanced braces in shell block"));
        }
        let raw = &self.source[open.start + 1..pos - 1];
        while self.current < self.tokens.len() && self.tokens[self.current].start < pos {
            self.current += 1;
        }

        if raw.contains("#{") {
            let parts = self.parse_interpolated_string(raw)?;
            Ok(Expr::InterpolatedString { parts, span: self.span_from_token(&open) })
        } else {
            Ok(Expr::Literal { value: Value::String(raw.trim().to_string()), span: self.span_from_token(&open) })
        }
    }

    fn parse_interpolated_string(&self, raw: &str) -> Result<Vec<InterpPart>, String> {
        let mut parts = Vec::new();
        let mut i = 0;
        while i < raw.len() {
            if let Some(start) = raw[i..].find("#{") {
                let lit = &raw[i..i + start];
                if !lit.is_empty() {
                    parts.push(InterpPart::Literal(lit.to_string()));
                }
                let mut j = i + start + 2;
                let mut depth = 1;
                while j < raw.len() && depth > 0 {
                    let ch = raw.as_bytes()[j] as char;
                    if ch == '{' { depth += 1; }
                    if ch == '}' { depth -= 1; }
                    j += 1;
                }
                if depth != 0 {
                    return Err("Unterminated interpolation".to_string());
                }
                let expr_text = &raw[i + start + 2..j - 1];
                let expr = parse_expression_from_str(expr_text, &self.filename)?;
                parts.push(InterpPart::Expr(expr));
                i = j;
            } else {
                let tail = &raw[i..];
                if !tail.is_empty() {
                    parts.push(InterpPart::Literal(tail.to_string()));
                }
                break;
            }
        }
        Ok(parts)
    }

    fn parse_arrow_lambda_single(&mut self) -> Result<Expr, String> {
        let span = self.prev_span();
        let param = self.consume_identifier("Expected parameter name")?;
        self.consume(&TokenKind::Arrow, "Expected '->' after parameter")?;
        let body = self.parse_lambda_body()?;
        Ok(Expr::Lambda {
            params: vec![ParamSpec { name: param, ty: None, default: None, variadic: false }],
            return_type: None,
            body,
            span,
        })
    }

    fn parse_arrow_lambda_multi(&mut self) -> Result<Expr, String> {
        let span = self.prev_span();
        self.consume(&TokenKind::LParen, "Expected '(' after lambda start")?;
        let params = self.parse_param_list()?;
        self.consume(&TokenKind::RParen, "Expected ')' after lambda params")?;
        self.consume(&TokenKind::Arrow, "Expected '->' after lambda params")?;
        let body = self.parse_lambda_body()?;
        Ok(Expr::Lambda { params, return_type: None, body, span })
    }

    fn parse_lambda_body(&mut self) -> Result<Vec<Stmt>, String> {
        if self.check(&TokenKind::LBrace) {
            return self.parse_block();
        }
        let expr = self.parse_expression()?;
        Ok(vec![Stmt::Return { value: Some(expr.clone()), span: expr.span() }])
    }

    fn parse_type(&mut self) -> Result<super::ast::TypeExpr, String> {
        let mut left = self.parse_type_primary()?;
        while self.match_kind(&TokenKind::Pipe) {
            let right = self.parse_type_primary()?;
            match left {
                super::ast::TypeExpr::Union(mut items) => {
                    items.push(right);
                    left = super::ast::TypeExpr::Union(items);
                }
                _ => {
                    left = super::ast::TypeExpr::Union(vec![left, right]);
                }
            }
        }
        Ok(left)
    }

    fn parse_type_primary(&mut self) -> Result<super::ast::TypeExpr, String> {
        let name = self.consume_identifier("Expected type name")?;
        let ty = match name.as_str() {
            "array" if self.check(&TokenKind::Less) => {
                self.consume(&TokenKind::Less, "Expected '<' after array")?;
                let inner = self.parse_type()?;
                self.consume(&TokenKind::Greater, "Expected '>' after array")?;
                super::ast::TypeExpr::Array(Box::new(inner))
            }
            "tuple" if self.check(&TokenKind::Less) => {
                self.consume(&TokenKind::Less, "Expected '<' after tuple")?;
                let mut elems = Vec::new();
                elems.push(self.parse_type()?);
                while self.match_kind(&TokenKind::Comma) {
                    elems.push(self.parse_type()?);
                }
                self.consume(&TokenKind::Greater, "Expected '>' after tuple")?;
                super::ast::TypeExpr::Tuple(elems)
            }
            "map" if self.check(&TokenKind::Less) => {
                self.consume(&TokenKind::Less, "Expected '<' after map")?;
                let key = self.parse_type()?;
                self.consume(&TokenKind::Comma, "Expected ',' in map type")?;
                let val = self.parse_type()?;
                self.consume(&TokenKind::Greater, "Expected '>' after map")?;
                super::ast::TypeExpr::Map(Box::new(key), Box::new(val))
            }
            "range" if self.check(&TokenKind::Less) => {
                self.consume(&TokenKind::Less, "Expected '<' after range")?;
                let inner = self.parse_type()?;
                self.consume(&TokenKind::Greater, "Expected '>' after range")?;
                super::ast::TypeExpr::Range(Box::new(inner))
            }
            "fn" if self.check(&TokenKind::Less) => {
                self.consume(&TokenKind::Less, "Expected '<' after fn")?;
                let mut args = Vec::new();
                if !self.check(&TokenKind::Greater) {
                    loop {
                        args.push(self.parse_type()?);
                        if !self.match_kind(&TokenKind::Comma) { break; }
                    }
                }
                self.consume(&TokenKind::Greater, "Expected '>' after fn")?;
                super::ast::TypeExpr::Fn(args)
            }
            _ => super::ast::TypeExpr::Simple(name),
        };
        Ok(ty)
    }

    fn parse_pattern_list(&mut self) -> Result<Pattern, String> {
        let mut parts = Vec::new();
        parts.push(self.parse_pattern()?);
        while self.match_kind(&TokenKind::Comma) {
            parts.push(self.parse_pattern()?);
        }
        if parts.len() == 1 {
            Ok(parts.remove(0))
        } else {
            Ok(Pattern::Tuple(parts))
        }
    }

    fn parse_pattern(&mut self) -> Result<Pattern, String> {
        if self.match_kind(&TokenKind::Underscore) {
            return Ok(Pattern::Ignore);
        }
        if self.match_kind(&TokenKind::LParen) {
            let mut parts = Vec::new();
            if !self.check(&TokenKind::RParen) {
                loop {
                    parts.push(self.parse_pattern()?);
                    if !self.match_kind(&TokenKind::Comma) { break; }
                }
            }
            self.consume(&TokenKind::RParen, "Expected ')' after pattern")?;
            return Ok(Pattern::Tuple(parts));
        }
        if self.match_kind(&TokenKind::Identifier(String::new())) {
            if let TokenKind::Identifier(name) = self.previous().kind.clone() {
                return Ok(Pattern::Name(name));
            }
        }
        Err(self.error_at_current("Invalid destructuring pattern"))
    }

    fn lookahead_has_comma_before_equal(&self) -> bool {
        let mut i = self.current;
        while i < self.tokens.len() {
            match self.tokens[i].kind {
                TokenKind::Equal => return false,
                TokenKind::Comma => return true,
                TokenKind::Eof => return false,
                _ => i += 1,
            }
        }
        false
    }

    fn try_parse_assignable(&mut self) -> Result<Expr, String> {
        let expr = self.parse_primary()?;
        let mut e = expr;
        while self.match_kind(&TokenKind::LBracket) {
            let index = self.parse_expression()?;
            self.consume(&TokenKind::RBracket, "Expected ']' after index")?;
            let span = e.span();
            e = Expr::Index { object: Box::new(e), index: Box::new(index), span };
        }
        Ok(e)
    }

    fn consume(&mut self, kind: &TokenKind, message: &str) -> Result<Token, String> {
        if self.check(kind) { return Ok(self.advance().clone()); }
        Err(self.error_at_current(message))
    }

    fn consume_identifier(&mut self, message: &str) -> Result<String, String> {
        if let TokenKind::Identifier(name) = self.peek().kind.clone() {
            self.advance();
            return Ok(name);
        }
        Err(self.error_at_current(message))
    }

    fn match_kind(&mut self, kind: &TokenKind) -> bool {
        if self.check(kind) { self.advance(); true } else { false }
    }

    fn match_any(&mut self, kinds: &[TokenKind]) -> bool {
        for k in kinds {
            if self.check(k) { self.advance(); return true; }
        }
        false
    }

    fn check(&self, kind: &TokenKind) -> bool {
        if self.is_at_end() { return false; }
        std::mem::discriminant(&self.peek().kind) == std::mem::discriminant(kind)
    }

    fn advance(&mut self) -> &Token {
        if !self.is_at_end() { self.current += 1; }
        self.previous()
    }

    fn is_at_end(&self) -> bool {
        matches!(self.peek().kind, TokenKind::Eof)
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.current]
    }

    fn peek_next(&self) -> &Token {
        if self.current + 1 >= self.tokens.len() { self.peek() } else { &self.tokens[self.current + 1] }
    }

    fn previous(&self) -> &Token {
        &self.tokens[self.current - 1]
    }

    fn previous_lexeme(&self) -> String {
        match &self.previous().kind {
            TokenKind::Plus => "+".to_string(),
            TokenKind::Minus => "-".to_string(),
            TokenKind::Star => "*".to_string(),
            TokenKind::Slash => "/".to_string(),
            TokenKind::Percent => "%".to_string(),
            TokenKind::EqualEqual => "==".to_string(),
            TokenKind::NotEqual => "!=".to_string(),
            TokenKind::Less => "<".to_string(),
            TokenKind::LessEqual => "<=".to_string(),
            TokenKind::Greater => ">".to_string(),
            TokenKind::GreaterEqual => ">=".to_string(),
            TokenKind::DotDot => "..".to_string(),
            TokenKind::Bang => "!".to_string(),
            TokenKind::AndAnd => "&&".to_string(),
            TokenKind::OrOr => "||".to_string(),
            TokenKind::Arrow => "->".to_string(),
            _ => "".to_string(),
        }
    }

    fn error_at_current(&self, message: &str) -> String {
        let tok = self.peek();
        format!(
            "{}:{}:{}: {message} (got {})",
            self.filename,
            tok.line,
            tok.column,
            token_desc(tok)
        )
    }

    fn prev_span(&self) -> Span {
        self.span_from_token(self.previous())
    }

    fn span_from_token(&self, tok: &Token) -> Span {
        Span { line: tok.line, col: tok.column }
    }

    fn peek_next_is_arrow(&self) -> bool {
        matches!(self.peek_next().kind, TokenKind::Arrow)
    }

    fn peek_next_is_equal(&self) -> bool {
        matches!(self.peek_next().kind, TokenKind::Equal)
    }

    fn next_is_arrow_after_params(&self) -> bool {
        let mut depth = 0i32;
        let mut i = self.current;
        while i < self.tokens.len() {
            let kind = &self.tokens[i].kind;
            match kind {
                TokenKind::LParen => depth += 1,
                TokenKind::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        if i + 1 < self.tokens.len() {
                            return matches!(self.tokens[i + 1].kind, TokenKind::Arrow);
                        }
                        return false;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        false
    }
}

fn parse_expression_from_str(expr_text: &str, filename: &str) -> Result<Expr, String> {
    let mut scanner = crate::lexer::scanner::Scanner::new(expr_text);
    let tokens = scanner.scan_tokens().map_err(|e| format!("{filename}: {e}"))?;
    let mut parser = Parser::new(tokens, expr_text, filename.to_string());
    parser.parse_expression()
}

fn contains_yield(stmts: &[Stmt]) -> bool {
    stmts.iter().any(stmt_contains_yield)
}

fn stmt_contains_yield(stmt: &Stmt) -> bool {
    match stmt {
        Stmt::Yield { .. } => true,
        Stmt::If {
            then_branch,
            else_branch,
            ..
        } => contains_yield(then_branch) || contains_yield(else_branch),
        Stmt::For { body, .. }
        | Stmt::ParallelFor { body, .. }
        | Stmt::While { body, .. }
        | Stmt::Defer { body, .. } => contains_yield(body),
        Stmt::Try {
            try_block,
            catch_block,
            finally_block,
            ..
        } => {
            contains_yield(try_block)
                || catch_block
                    .as_ref()
                    .map(|b| contains_yield(b))
                    .unwrap_or(false)
                || finally_block
                    .as_ref()
                    .map(|b| contains_yield(b))
                    .unwrap_or(false)
        }
        Stmt::Expr { expr, .. } => expr_contains_yield(expr),
        Stmt::Let { expr, .. }
        | Stmt::LetDestructure { expr, .. }
        | Stmt::Assign { expr, .. }
        | Stmt::Return {
            value: Some(expr), ..
        }
        | Stmt::Throw { value: expr, .. }
        | Stmt::Invoke { expr, .. } => expr_contains_yield(expr),
        Stmt::IndexAssign {
            target, index, expr, ..
        } => {
            expr_contains_yield(target) || expr_contains_yield(index) || expr_contains_yield(expr)
        }
        Stmt::Return { value: None, .. }
        | Stmt::Continue { .. }
        | Stmt::Break { .. }
        | Stmt::TypeAlias { .. }
        | Stmt::RecordDef { .. }
        | Stmt::Function { .. } => false,
    }
}

fn expr_contains_yield(expr: &Expr) -> bool {
    match expr {
        Expr::Binary { left, right, .. } => expr_contains_yield(left) || expr_contains_yield(right),
        Expr::Unary { expr, .. } => expr_contains_yield(expr),
        Expr::Call { callee, args, .. } => {
            expr_contains_yield(callee)
                || args.iter().any(|arg| match arg {
                    CallArg::Positional(e) | CallArg::Spread(e) => expr_contains_yield(e),
                    CallArg::Named { value, .. } => expr_contains_yield(value),
                })
        }
        Expr::Member { object, .. } => expr_contains_yield(object),
        Expr::Index { object, index, .. } => expr_contains_yield(object) || expr_contains_yield(index),
        Expr::Array { items, .. } | Expr::Tuple { items, .. } => {
            items.iter().any(expr_contains_yield)
        }
        Expr::Dict { items, .. } => items
            .iter()
            .any(|(k, v)| expr_contains_yield(k) || expr_contains_yield(v)),
        Expr::ArrayComprehension {
            iterable,
            guard,
            map_expr,
            ..
        } => {
            expr_contains_yield(iterable)
                || guard
                    .as_ref()
                    .map(|g| expr_contains_yield(g))
                    .unwrap_or(false)
                || expr_contains_yield(map_expr)
        }
        Expr::Range { start, end, .. } => expr_contains_yield(start) || expr_contains_yield(end),
        Expr::IfExpr {
            cond,
            then_branch,
            else_branch,
            ..
        } => expr_contains_yield(cond) || contains_yield(then_branch) || contains_yield(else_branch),
        Expr::Match { subject, arms, .. } => {
            expr_contains_yield(subject)
                || arms.iter().any(|arm| match &arm.kind {
                    MatchArmKind::Value(e) => expr_contains_yield(e) || contains_yield(&arm.body),
                    MatchArmKind::Compare { rhs, .. } => expr_contains_yield(rhs) || contains_yield(&arm.body),
                    MatchArmKind::Wildcard => contains_yield(&arm.body),
                })
        }
        Expr::ParallelFor { iterable, body, .. } => expr_contains_yield(iterable) || contains_yield(body),
        Expr::TaskBlock { .. } => false,
        Expr::TaskCall { callee, args, .. } => {
            expr_contains_yield(callee)
                || args.iter().any(|arg| match arg {
                    CallArg::Positional(e) | CallArg::Spread(e) => expr_contains_yield(e),
                    CallArg::Named { value, .. } => expr_contains_yield(value),
                })
        }
        Expr::Await { expr, .. } => expr_contains_yield(expr),
        Expr::Lambda { .. } => false,
        Expr::InterpolatedString { parts, .. } => parts.iter().any(|p| match p {
            InterpPart::Literal(_) => false,
            InterpPart::Expr(e) => expr_contains_yield(e),
        }),
        Expr::Sh { command, .. } => expr_contains_yield(command),
        Expr::Ssh { host, command, .. } => expr_contains_yield(host) || expr_contains_yield(command),
        Expr::Literal { .. } | Expr::Var { .. } => false,
    }
}

fn token_desc(tok: &Token) -> String {
    match &tok.kind {
        TokenKind::Identifier(s) => format!("Identifier({s})"),
        TokenKind::Number(s) => format!("Number({s})"),
        TokenKind::String(s) => format!("String({})", shorten(s)),
        TokenKind::InterpolatedString(s) => format!("InterpolatedString({})", shorten(s)),
        TokenKind::TimeLiteral(s) => format!("TimeLiteral({s})"),
        other => format!("{other:?}"),
    }
}

fn shorten(s: &str) -> String {
    if s.len() > 16 {
        format!("{}...", &s[..16])
    } else {
        s.to_string()
    }
}
