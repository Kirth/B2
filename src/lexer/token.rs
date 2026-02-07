#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // Keywords
    Let,
    Fn,
    Return,
    If,
    Else,
    For,
    While,
    True,
    False,
    Log,
    Echo,
    Sh,
    Ssh,
    Parallel,
    Task,
    Await,
    Try,
    Catch,

    // Operators / punctuation
    LParen,
    RParen,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Comma,
    Colon,
    Semicolon,
    Dot,
    DotDot,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Equal,
    EqualEqual,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Bang,
    Question,
    AndAnd,
    OrOr,
    Arrow,
    Pipe,
    Underscore,

    // Literals
    Identifier(String),
    Number(String),
    String(String),
    InterpolatedString(String),
    TimeLiteral(String),

    Eof,
}

#[derive(Debug, Clone)]
pub struct Token {
    pub kind: TokenKind,
    pub line: usize,
    pub column: usize,
    pub start: usize,
}

impl Token {
    pub fn new(kind: TokenKind, line: usize, column: usize, start: usize) -> Self {
        Token {
            kind,
            line,
            column,
            start,
        }
    }
}
