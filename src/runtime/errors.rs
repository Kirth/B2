use std::fmt;

#[derive(Debug, Clone)]
pub struct Frame {
    pub name: String,
    pub line: usize,
    pub col: usize,
}

#[derive(Debug, Clone)]
pub struct RuntimeError {
    pub message: String,
    pub filename: String,
    pub line: usize,
    pub col: usize,
    pub snippet: Option<String>,
    pub stack: Vec<Frame>,
}

impl RuntimeError {
    pub fn new(message: String, filename: String, line: usize, col: usize, snippet: Option<String>, stack: Vec<Frame>) -> Self {
        RuntimeError { message, filename, line, col, snippet, stack }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}:{}:{}: {}", self.filename, self.line, self.col, self.message)?;
        if let Some(snippet) = &self.snippet {
            writeln!(f, "Snippet:\n{}", snippet)?;
        }
        if !self.stack.is_empty() {
            writeln!(f, "Stack trace:")?;
            for frame in &self.stack {
                writeln!(f, "  at {} ({}:{}:{})", frame.name, self.filename, frame.line, frame.col)?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for RuntimeError {}
