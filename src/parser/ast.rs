use crate::runtime::value::Value;

#[derive(Debug, Clone, Copy)]
pub struct Span {
    pub line: usize,
    pub col: usize,
}

#[derive(Debug, Clone)]
pub enum Pattern {
    Name(String),
    Ignore,
    Tuple(Vec<Pattern>),
}

#[derive(Debug, Clone)]
pub enum TypeExpr {
    Simple(String),
    Array(Box<TypeExpr>),
    Tuple(Vec<TypeExpr>),
    Map(Box<TypeExpr>, Box<TypeExpr>),
    Range(Box<TypeExpr>),
    Fn(Vec<TypeExpr>),
    Union(Vec<TypeExpr>),
}

#[derive(Debug, Clone)]
pub struct Program {
    pub statements: Vec<Stmt>,
}

#[derive(Debug, Clone)]
pub struct RecordField {
    pub name: String,
    pub ty: TypeExpr,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct ImportItem {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ImportSource {
    Builtin(String),
    Path(String),
}

#[derive(Debug, Clone)]
pub enum Stmt {
    Expr {
        expr: Expr,
        span: Span,
    },
    Let {
        name: String,
        expr: Expr,
        span: Span,
    },
    LetDestructure {
        pattern: Pattern,
        expr: Expr,
        span: Span,
    },
    Assign {
        name: String,
        expr: Expr,
        span: Span,
    },
    IndexAssign {
        target: Expr,
        index: Expr,
        expr: Expr,
        span: Span,
    },
    If {
        cond: Expr,
        then_branch: Vec<Stmt>,
        else_branch: Vec<Stmt>,
        span: Span,
    },
    For {
        pattern: Pattern,
        iterable: Expr,
        body: Vec<Stmt>,
        span: Span,
    },
    ParallelFor {
        pattern: Pattern,
        iterable: Expr,
        body: Vec<Stmt>,
        span: Span,
    },
    While {
        cond: Expr,
        body: Vec<Stmt>,
        span: Span,
    },
    Try {
        try_block: Vec<Stmt>,
        catch_name: Option<String>,
        catch_block: Option<Vec<Stmt>>,
        finally_block: Option<Vec<Stmt>>,
        span: Span,
    },
    Function {
        name: String,
        params: Vec<ParamSpec>,
        return_type: Option<TypeExpr>,
        body: Vec<Stmt>,
        is_generator: bool,
        span: Span,
    },
    Return {
        value: Option<Expr>,
        span: Span,
    },
    Yield {
        value: Option<Expr>,
        span: Span,
    },
    Continue {
        span: Span,
    },
    Break {
        span: Span,
    },
    Throw {
        value: Expr,
        span: Span,
    },
    Defer {
        body: Vec<Stmt>,
        span: Span,
    },
    Use {
        module: String,
        alias: Option<String>,
        span: Span,
    },
    ImportNamed {
        items: Vec<ImportItem>,
        source: ImportSource,
        span: Span,
    },
    ImportNamespace {
        alias: String,
        source: ImportSource,
        span: Span,
    },
    TypeAlias {
        name: String,
        target: TypeExpr,
        span: Span,
    },
    RecordDef {
        name: String,
        fields: Vec<RecordField>,
        span: Span,
    },
    Invoke {
        name: String,
        expr: Expr,
        span: Span,
    },
}

#[derive(Debug, Clone)]
pub enum Expr {
    Literal {
        value: Value,
        span: Span,
    },
    Var {
        name: String,
        span: Span,
    },
    Binary {
        left: Box<Expr>,
        op: String,
        right: Box<Expr>,
        span: Span,
    },
    Unary {
        op: String,
        expr: Box<Expr>,
        span: Span,
    },
    Call {
        callee: Box<Expr>,
        args: Vec<CallArg>,
        span: Span,
    },
    Member {
        object: Box<Expr>,
        name: String,
        span: Span,
    },
    NamespaceMember {
        object: Box<Expr>,
        name: String,
        span: Span,
    },
    Index {
        object: Box<Expr>,
        index: Box<Expr>,
        span: Span,
    },
    Array {
        items: Vec<Expr>,
        span: Span,
    },
    Tuple {
        items: Vec<Expr>,
        span: Span,
    },
    Dict {
        items: Vec<(Expr, Expr)>,
        span: Span,
    },
    ArrayComprehension {
        pattern: Pattern,
        iterable: Box<Expr>,
        guard: Option<Box<Expr>>,
        map_expr: Box<Expr>,
        span: Span,
    },
    Range {
        start: Box<Expr>,
        end: Box<Expr>,
        span: Span,
    },
    IfExpr {
        cond: Box<Expr>,
        then_branch: Vec<Stmt>,
        else_branch: Vec<Stmt>,
        span: Span,
    },
    Match {
        subject: Box<Expr>,
        arms: Vec<MatchArm>,
        span: Span,
    },
    ParallelFor {
        pattern: Pattern,
        iterable: Box<Expr>,
        body: Vec<Stmt>,
        span: Span,
    },
    TaskBlock {
        body: Vec<Stmt>,
        span: Span,
    },
    TaskCall {
        callee: Box<Expr>,
        args: Vec<CallArg>,
        span: Span,
    },
    Await {
        expr: Box<Expr>,
        safe: bool,
        span: Span,
    },
    Lambda {
        params: Vec<ParamSpec>,
        return_type: Option<TypeExpr>,
        body: Vec<Stmt>,
        span: Span,
    },
    InterpolatedString {
        parts: Vec<InterpPart>,
        span: Span,
    },
    Sh {
        command: Box<Expr>,
        span: Span,
    },
    Ssh {
        host: Box<Expr>,
        command: Box<Expr>,
        span: Span,
    },
}

#[derive(Debug, Clone)]
pub enum InterpPart {
    Literal(String),
    Expr(Expr),
}

#[derive(Debug, Clone)]
pub struct ParamSpec {
    pub name: String,
    pub ty: Option<TypeExpr>,
    pub default: Option<Expr>,
    pub variadic: bool,
}

#[derive(Debug, Clone)]
pub enum CallArg {
    Positional(Expr),
    Spread(Expr),
    Named { name: String, value: Expr },
}

#[derive(Debug, Clone)]
pub struct MatchArm {
    pub kind: MatchArmKind,
    pub body: Vec<Stmt>,
}

#[derive(Debug, Clone)]
pub enum MatchArmKind {
    Value(Expr),
    Compare { op: String, rhs: Expr },
    Wildcard,
}

impl Stmt {
    pub fn span(&self) -> Span {
        match self {
            Stmt::Expr { span, .. }
            | Stmt::Let { span, .. }
            | Stmt::LetDestructure { span, .. }
            | Stmt::Assign { span, .. }
            | Stmt::IndexAssign { span, .. }
            | Stmt::If { span, .. }
            | Stmt::For { span, .. }
            | Stmt::ParallelFor { span, .. }
            | Stmt::While { span, .. }
            | Stmt::Try { span, .. }
            | Stmt::Function { span, .. }
            | Stmt::Return { span, .. }
            | Stmt::Yield { span, .. }
            | Stmt::Continue { span, .. }
            | Stmt::Break { span, .. }
            | Stmt::Throw { span, .. }
            | Stmt::Defer { span, .. }
            | Stmt::Use { span, .. }
            | Stmt::ImportNamed { span, .. }
            | Stmt::ImportNamespace { span, .. }
            | Stmt::TypeAlias { span, .. }
            | Stmt::RecordDef { span, .. }
            | Stmt::Invoke { span, .. } => *span,
        }
    }
}

impl Expr {
    pub fn span(&self) -> Span {
        match self {
            Expr::Literal { span, .. }
            | Expr::Var { span, .. }
            | Expr::Binary { span, .. }
            | Expr::Unary { span, .. }
            | Expr::Call { span, .. }
            | Expr::Member { span, .. }
            | Expr::NamespaceMember { span, .. }
            | Expr::Index { span, .. }
            | Expr::Array { span, .. }
            | Expr::Tuple { span, .. }
            | Expr::Dict { span, .. }
            | Expr::ArrayComprehension { span, .. }
            | Expr::Range { span, .. }
            | Expr::IfExpr { span, .. }
            | Expr::Match { span, .. }
            | Expr::ParallelFor { span, .. }
            | Expr::TaskBlock { span, .. }
            | Expr::TaskCall { span, .. }
            | Expr::Await { span, .. }
            | Expr::Lambda { span, .. }
            | Expr::InterpolatedString { span, .. }
            | Expr::Sh { span, .. }
            | Expr::Ssh { span, .. } => *span,
        }
    }
}
