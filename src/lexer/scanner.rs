use super::token::{Token, TokenKind};

pub struct Scanner<'a> {
    source: &'a str,
    chars: Vec<char>,
    start: usize,
    current: usize,
    line: usize,
    column: usize,
}

impl<'a> Scanner<'a> {
    pub fn new(source: &'a str) -> Self {
        Scanner {
            source,
            chars: source.chars().collect(),
            start: 0,
            current: 0,
            line: 1,
            column: 1,
        }
    }

    pub fn scan_tokens(&mut self) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();
        while !self.is_at_end() {
            self.start = self.current;
            match self.scan_token()? {
                Some(tok) => tokens.push(tok),
                None => {}
            }
        }
        tokens.push(Token::new(TokenKind::Eof, self.line, self.column, self.current));
        Ok(tokens)
    }

    fn scan_token(&mut self) -> Result<Option<Token>, String> {
        let c = self.advance();
        match c {
            ' ' | '\r' | '\t' => Ok(None),
            '\n' => {
                self.line += 1;
                self.column = 1;
                Ok(None)
            }
            '(' => Ok(Some(self.simple(TokenKind::LParen))),
            ')' => Ok(Some(self.simple(TokenKind::RParen))),
            '{' => Ok(Some(self.simple(TokenKind::LBrace))),
            '}' => Ok(Some(self.simple(TokenKind::RBrace))),
            '[' => Ok(Some(self.simple(TokenKind::LBracket))),
            ']' => Ok(Some(self.simple(TokenKind::RBracket))),
            ',' => Ok(Some(self.simple(TokenKind::Comma))),
            ':' => Ok(Some(self.simple(TokenKind::Colon))),
            ';' => Ok(Some(self.simple(TokenKind::Semicolon))),
            '.' => {
                if self.match_char('.') {
                    if self.match_char('.') {
                        Ok(Some(self.simple(TokenKind::Ellipsis)))
                    } else {
                        Ok(Some(self.simple(TokenKind::DotDot)))
                    }
                } else {
                    Ok(Some(self.simple(TokenKind::Dot)))
                }
            }
            '+' => Ok(Some(self.simple(TokenKind::Plus))),
            '-' => {
                if self.match_char('>') {
                    Ok(Some(self.simple(TokenKind::Arrow)))
                } else {
                    Ok(Some(self.simple(TokenKind::Minus)))
                }
            }
            '*' => Ok(Some(self.simple(TokenKind::Star))),
            '/' => Ok(Some(self.simple(TokenKind::Slash))),
            '%' => Ok(Some(self.simple(TokenKind::Percent))),
            '?' => Ok(Some(self.simple(TokenKind::Question))),
            '_' => Ok(Some(self.simple(TokenKind::Underscore))),
            '|' => {
                if self.match_char('|') {
                    Ok(Some(self.simple(TokenKind::OrOr)))
                } else {
                    Ok(Some(self.simple(TokenKind::Pipe)))
                }
            }
            '&' => {
                if self.match_char('&') {
                    Ok(Some(self.simple(TokenKind::AndAnd)))
                } else {
                    Ok(None)
                }
            }
            '!' => {
                if self.match_char('=') {
                    Ok(Some(self.simple(TokenKind::NotEqual)))
                } else {
                    Ok(Some(self.simple(TokenKind::Bang)))
                }
            }
            '=' => {
                if self.match_char('=') {
                    Ok(Some(self.simple(TokenKind::EqualEqual)))
                } else if self.match_char('>') {
                    Ok(Some(self.simple(TokenKind::FatArrow)))
                } else {
                    Ok(Some(self.simple(TokenKind::Equal)))
                }
            }
            '<' => {
                if self.match_char('=') {
                    Ok(Some(self.simple(TokenKind::LessEqual)))
                } else {
                    Ok(Some(self.simple(TokenKind::Less)))
                }
            }
            '>' => {
                if self.match_char('=') {
                    Ok(Some(self.simple(TokenKind::GreaterEqual)))
                } else {
                    Ok(Some(self.simple(TokenKind::Greater)))
                }
            }
            '#' => {
                while !self.is_at_end() && self.peek() != '\n' {
                    self.advance();
                }
                Ok(None)
            }
            '"' => self.scan_string(),
            c if c.is_ascii_digit() => self.scan_number(c),
            c if is_alpha(c) => self.scan_identifier(c),
            _ => Ok(None),
        }
    }

    fn scan_string(&mut self) -> Result<Option<Token>, String> {
        let start_pos = self.current;
        let mut has_interpolation = false;
        let mut has_escape = false;

        while !self.is_at_end() && self.peek() != '"' {
            if self.peek() == '#' && self.peek_next() == '{' {
                has_interpolation = true;
            }
            if self.peek() == '\\' {
                has_escape = true;
                self.advance();
            }
            if self.peek() == '\n' {
                self.line += 1;
                self.column = 1;
            }
            self.advance();
        }

        if self.is_at_end() {
            return Err(format!("Unterminated string at line {}", self.line));
        }

        let end_pos = self.current;
        self.advance(); // closing quote

        let raw = &self.source[start_pos..end_pos];
        let value = if has_escape {
            unescape_string(raw)
        } else {
            raw.to_string()
        };

        let kind = if has_interpolation {
            TokenKind::InterpolatedString(value)
        } else {
            TokenKind::String(value)
        };

        Ok(Some(self.token_from(kind)))
    }

    fn scan_number(&mut self, _first: char) -> Result<Option<Token>, String> {
        while self.peek().is_ascii_digit() {
            self.advance();
        }
        if self.peek() == '.' && self.peek_next().is_ascii_digit() {
            self.advance();
            while self.peek().is_ascii_digit() {
                self.advance();
            }
        }

        if self.peek().is_ascii_alphabetic() {
            let suffix_start = self.current;
            while self.peek().is_ascii_alphabetic() {
                self.advance();
            }
            let slice = &self.source[self.start..self.current];
            if is_time_literal(slice) {
                return Ok(Some(self.token_from(TokenKind::TimeLiteral(slice.to_string()))));
            }
            self.current = suffix_start;
        }

        let slice = &self.source[self.start..self.current];
        Ok(Some(self.token_from(TokenKind::Number(slice.to_string()))))
    }

    fn scan_identifier(&mut self, _first: char) -> Result<Option<Token>, String> {
        while is_alphanumeric(self.peek()) || self.peek() == '_' {
            self.advance();
        }
        let text = &self.source[self.start..self.current];
        let kind = match text {
            "let" => TokenKind::Let,
            "fn" => TokenKind::Fn,
            "return" => TokenKind::Return,
            "continue" => TokenKind::Continue,
            "break" => TokenKind::Break,
            "throw" => TokenKind::Throw,
            "if" => TokenKind::If,
            "else" => TokenKind::Else,
            "for" => TokenKind::For,
            "while" => TokenKind::While,
            "true" => TokenKind::True,
            "false" => TokenKind::False,
            "log" => TokenKind::Log,
            "echo" => TokenKind::Echo,
            "sh" => TokenKind::Sh,
            "ssh" => TokenKind::Ssh,
            "parallel" => TokenKind::Parallel,
            "task" => TokenKind::Task,
            "await" => TokenKind::Await,
            "match" => TokenKind::Match,
            "try" => TokenKind::Try,
            "catch" => TokenKind::Catch,
            "finally" => TokenKind::Finally,
            "defer" => TokenKind::Defer,
            _ => TokenKind::Identifier(text.to_string()),
        };
        Ok(Some(self.token_from(kind)))
    }

    fn token_from(&self, kind: TokenKind) -> Token {
        Token::new(kind, self.line, self.column, self.start)
    }

    fn simple(&self, kind: TokenKind) -> Token {
        Token::new(kind, self.line, self.column, self.start)
    }

    fn is_at_end(&self) -> bool {
        self.current >= self.chars.len()
    }

    fn advance(&mut self) -> char {
        let c = self.chars[self.current];
        self.current += 1;
        self.column += 1;
        c
    }

    fn peek(&self) -> char {
        if self.is_at_end() { '\0' } else { self.chars[self.current] }
    }

    fn peek_next(&self) -> char {
        if self.current + 1 >= self.chars.len() { '\0' } else { self.chars[self.current + 1] }
    }

    fn match_char(&mut self, expected: char) -> bool {
        if self.is_at_end() { return false; }
        if self.chars[self.current] != expected { return false; }
        self.current += 1;
        self.column += 1;
        true
    }
}

fn is_alpha(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_alphanumeric(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn unescape_string(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                match next {
                    'n' => out.push('\n'),
                    't' => out.push('\t'),
                    'r' => out.push('\r'),
                    '\\' => out.push('\\'),
                    '"' => out.push('"'),
                    '#' => out.push('#'),
                    _ => out.push(next),
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn is_time_literal(s: &str) -> bool {
    let mut idx = 0;
    for c in s.chars() {
        if c.is_ascii_digit() { idx += 1; } else { break; }
    }
    if idx == 0 || idx >= s.len() { return false; }
    let unit = &s[idx..];
    matches!(unit, "ms" | "s" | "m" | "h" | "d")
}
