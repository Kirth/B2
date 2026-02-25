use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::{Paragraph, Wrap};

use crate::lexer::scanner::Scanner;
use crate::parser::parser::Parser;
use crate::runtime::builtins::register_builtins;
use crate::runtime::executor::Executor;

pub fn run() -> Result<(), String> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return run_line_repl();
    }

    let pending_output = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut executor = Executor::new("<repl>".to_string(), "".to_string());
    register_builtins(&mut executor);
    {
        let output = pending_output.clone();
        executor.set_output_sink(Some(Arc::new(move |line: String| {
            if let Ok(mut q) = output.lock() {
                q.push(line);
            }
        })));
    }

    let mut app = ReplApp::new(executor, pending_output);

    enable_raw_mode().map_err(|e| format!("Failed to enable raw mode: {e}"))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)
        .map_err(|e| format!("Failed to enter alt screen: {e}"))?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal =
        Terminal::new(backend).map_err(|e| format!("Failed to create terminal: {e}"))?;

    let run_result = tui_loop(&mut terminal, &mut app);

    disable_raw_mode().map_err(|e| format!("Failed to disable raw mode: {e}"))?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)
        .map_err(|e| format!("Failed to leave alt screen: {e}"))?;
    terminal
        .show_cursor()
        .map_err(|e| format!("Failed to restore cursor: {e}"))?;

    app.save_history()?;
    run_result
}

#[derive(Clone)]
enum EntryKind {
    Input,
    Output,
    Result,
    Error,
    Info,
}

struct ReplEntry {
    kind: EntryKind,
    text: String,
    exec_id: Option<u64>,
}

struct ReplApp {
    executor: Executor,
    pending_output: Arc<Mutex<Vec<String>>>,
    entries: Vec<ReplEntry>,
    input: String,
    input_cursor: usize,
    history: Vec<String>,
    history_path: PathBuf,
    timing_enabled: bool,
    should_quit: bool,
    output_scroll: u16,
    follow_output: bool,
    next_exec_id: u64,
    output_exec_hint: Option<u64>,
}

impl ReplApp {
    fn new(executor: Executor, pending_output: Arc<Mutex<Vec<String>>>) -> Self {
        let history_path = history_path();
        let history = load_history(&history_path).unwrap_or_default();
        let mut app = Self {
            executor,
            pending_output,
            entries: Vec::new(),
            input: String::new(),
            input_cursor: 0,
            history,
            history_path,
            timing_enabled: false,
            should_quit: false,
            output_scroll: 0,
            follow_output: true,
            next_exec_id: 1,
            output_exec_hint: None,
        };
        app.push_info("v2r repl");
        app.push_info(
            "Notebook mode: Enter newline | Ctrl+R / Ctrl+Enter / F5 run cell | Ctrl+D quit",
        );
        app
    }

    fn save_history(&self) -> Result<(), String> {
        save_history_to(&self.history_path, &self.history)
    }

    fn push_entry(&mut self, kind: EntryKind, text: impl Into<String>) {
        self.push_entry_with_exec(kind, text, None);
    }

    fn push_entry_with_exec(
        &mut self,
        kind: EntryKind,
        text: impl Into<String>,
        exec_id: Option<u64>,
    ) {
        self.entries.push(ReplEntry {
            kind,
            text: text.into(),
            exec_id,
        });
        self.follow_output = true;
    }

    fn push_info(&mut self, text: impl Into<String>) {
        self.push_entry(EntryKind::Info, text);
    }

    fn push_error(&mut self, text: impl Into<String>) {
        self.push_entry(EntryKind::Error, text);
    }

    fn drain_output(&mut self) {
        let lines = if let Ok(mut out) = self.pending_output.lock() {
            std::mem::take(&mut *out)
        } else {
            Vec::new()
        };
        for line in lines {
            self.push_entry_with_exec(EntryKind::Output, line, self.output_exec_hint);
        }
    }

    fn push_history(&mut self, line: String) {
        if !line.trim().is_empty() && !self.history.last().map(|s| s == &line).unwrap_or(false) {
            self.history.push(line);
        }
    }

    fn submit(&mut self) {
        let src = self.input.trim_end().to_string();
        self.input.clear();
        self.input_cursor = 0;
        if src.trim().is_empty() {
            return;
        }

        let exec_id = self.next_exec_id;
        self.next_exec_id += 1;
        self.push_entry_with_exec(EntryKind::Input, src.clone(), Some(exec_id));
        self.push_history(src.clone());
        self.output_exec_hint = Some(exec_id);

        if src.starts_with(':') {
            self.handle_command(&src);
        } else {
            self.eval_source("<repl>".to_string(), src, true, exec_id);
        }

        self.drain_output();
        self.output_exec_hint = None;
    }

    fn eval_source(
        &mut self,
        filename: String,
        source: String,
        auto_print_result: bool,
        exec_id: u64,
    ) {
        let start = Instant::now();
        self.executor.set_context(filename.clone(), source.clone());

        let mut scanner = Scanner::new(&source);
        let tokens = match scanner.scan_tokens() {
            Ok(v) => v,
            Err(err) => {
                self.push_error(format!("{filename}: {err}"));
                return;
            }
        };

        let mut parser = Parser::new(tokens, &source, filename.clone());
        let program = match parser.parse_program() {
            Ok(v) => v,
            Err(err) => {
                self.push_error(err);
                return;
            }
        };

        match self.executor.execute_repl(&program) {
            Ok(last) => {
                if auto_print_result {
                    if let Some(v) = last {
                        self.push_entry_with_exec(EntryKind::Result, v.as_string(), Some(exec_id));
                    }
                }
                if self.timing_enabled {
                    self.push_entry_with_exec(
                        EntryKind::Info,
                        format!("time: {} ms", start.elapsed().as_millis()),
                        Some(exec_id),
                    );
                }
            }
            Err(err) => self.push_entry_with_exec(EntryKind::Error, err.to_string(), Some(exec_id)),
        }
    }

    fn handle_command(&mut self, line: &str) {
        let rest = line.trim_start_matches(':').trim();
        if rest.is_empty() {
            self.push_error("Empty command. Use :help");
            return;
        }
        let mut parts = rest.splitn(2, char::is_whitespace);
        let cmd = parts.next().unwrap_or("");
        let arg = parts.next().unwrap_or("").trim();

        match cmd {
            "help" => self.push_info(
                ":help :quit :clear :vars :load <file> :ast <code> :tokens <code> :time <on|off> :history export <path> :history import <path>",
            ),
            "quit" | "q" | "exit" => self.should_quit = true,
            "clear" => {
                self.entries.clear();
                self.output_scroll = 0;
                self.follow_output = true;
            }
            "vars" => {
                let vars = self.executor.list_globals();
                if vars.is_empty() {
                    self.push_info("No user globals");
                } else {
                    for (name, value) in vars {
                        self.push_info(format!("{name} = {}", value.as_string()));
                    }
                }
            }
            "load" => {
                if arg.is_empty() {
                    self.push_error("Usage: :load <file>");
                } else {
                    match fs::read_to_string(arg) {
                        Ok(src) => {
                            let exec_id = self.next_exec_id;
                            self.next_exec_id += 1;
                            self.push_entry_with_exec(
                                EntryKind::Input,
                                format!(":load {arg}"),
                                Some(exec_id),
                            );
                            self.eval_source(arg.to_string(), src, false, exec_id);
                        }
                        Err(err) => self.push_error(format!("Failed to read {arg}: {err}")),
                    }
                }
            }
            "ast" => {
                if arg.is_empty() {
                    self.push_error("Usage: :ast <code>");
                } else {
                    let mut scanner = Scanner::new(arg);
                    match scanner.scan_tokens() {
                        Ok(tokens) => {
                            let mut parser = Parser::new(tokens, arg, "<repl>".to_string());
                            match parser.parse_program() {
                                Ok(program) => self.push_info(format!("{program:#?}")),
                                Err(err) => self.push_error(err),
                            }
                        }
                        Err(err) => self.push_error(format!("<repl>: {err}")),
                    }
                }
            }
            "tokens" => {
                if arg.is_empty() {
                    self.push_error("Usage: :tokens <code>");
                } else {
                    let mut scanner = Scanner::new(arg);
                    match scanner.scan_tokens() {
                        Ok(tokens) => {
                            for t in tokens {
                                self.push_info(format!("{}:{} {:?}", t.line, t.column, t.kind));
                            }
                        }
                        Err(err) => self.push_error(format!("<repl>: {err}")),
                    }
                }
            }
            "time" => {
                if arg == "on" {
                    self.timing_enabled = true;
                    self.push_info("Timing enabled");
                } else if arg == "off" {
                    self.timing_enabled = false;
                    self.push_info("Timing disabled");
                } else {
                    self.push_error("Usage: :time <on|off>");
                }
            }
            "history" => {
                let mut hparts = arg.splitn(2, char::is_whitespace);
                let sub = hparts.next().unwrap_or("");
                let path = hparts.next().unwrap_or("").trim();
                match sub {
                    "export" => {
                        if path.is_empty() {
                            self.push_error("Usage: :history export <path>");
                        } else {
                            match save_history_to(path, &self.history) {
                                Ok(_) => self.push_info(format!("History exported to {path}")),
                                Err(err) => self.push_error(err),
                            }
                        }
                    }
                    "import" => {
                        if path.is_empty() {
                            self.push_error("Usage: :history import <path>");
                        } else {
                            match load_history(Path::new(path)) {
                                Ok(hist) => {
                                    let count = hist.len();
                                    self.history = hist;
                                    self.push_info(format!("Imported {count} history entries"));
                                }
                                Err(err) => self.push_error(err),
                            }
                        }
                    }
                    _ => self.push_error("Usage: :history <export|import> <path>"),
                }
            }
            _ => self.push_error(format!("Unknown command: :{cmd}")),
        }
    }

    fn notebook_lines(&self) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        for entry in &self.entries {
            let (prefix, style) = match entry.kind {
                EntryKind::Input => (
                    format!("In [{}]: ", entry.exec_id.unwrap_or(0)),
                    Style::default().fg(Color::Cyan),
                ),
                EntryKind::Output => (
                    format!("  [{}]  ", entry.exec_id.unwrap_or(0)),
                    Style::default().fg(Color::White),
                ),
                EntryKind::Result => (
                    format!("Out[{}]: ", entry.exec_id.unwrap_or(0)),
                    Style::default().fg(Color::Green),
                ),
                EntryKind::Error => (
                    format!("Err[{}]: ", entry.exec_id.unwrap_or(0)),
                    Style::default().fg(Color::Red),
                ),
                EntryKind::Info => ("info: ".to_string(), Style::default().fg(Color::Yellow)),
            };
            let mut iter = entry.text.lines();
            if let Some(first) = iter.next() {
                lines.push(Line::styled(format!("{prefix}{first}"), style));
            }
            for cont in iter {
                lines.push(Line::styled(
                    format!("{}{}", " ".repeat(prefix.len()), cont),
                    style,
                ));
            }
        }
        let draft_prefix = format!("In [{}]: ", self.next_exec_id);
        let draft_style = Style::default().fg(Color::Cyan);
        if self.input.is_empty() {
            lines.push(Line::styled(draft_prefix, draft_style));
        } else {
            let mut iter = self.input.lines();
            if let Some(first) = iter.next() {
                lines.push(Line::styled(format!("{draft_prefix}{first}"), draft_style));
            }
            for cont in iter {
                lines.push(Line::styled(
                    format!("{}{}", " ".repeat(draft_prefix.len()), cont),
                    draft_style,
                ));
            }
        }
        if lines.is_empty() {
            lines.push(Line::styled("", Style::default()));
        }
        lines
    }

    fn insert_str_at_cursor(&mut self, text: &str) {
        let byte_idx = char_to_byte_idx(&self.input, self.input_cursor);
        self.input.insert_str(byte_idx, text);
        self.input_cursor += text.chars().count();
    }

    fn backspace_at_cursor(&mut self) {
        if self.input_cursor == 0 {
            return;
        }
        let start_char = self.input_cursor - 1;
        let start_byte = char_to_byte_idx(&self.input, start_char);
        let end_byte = char_to_byte_idx(&self.input, self.input_cursor);
        self.input.replace_range(start_byte..end_byte, "");
        self.input_cursor = start_char;
    }

    fn delete_at_cursor(&mut self) {
        let len = self.input.chars().count();
        if self.input_cursor >= len {
            return;
        }
        let start_byte = char_to_byte_idx(&self.input, self.input_cursor);
        let end_byte = char_to_byte_idx(&self.input, self.input_cursor + 1);
        self.input.replace_range(start_byte..end_byte, "");
    }

    fn move_left(&mut self) {
        if self.input_cursor > 0 {
            self.input_cursor -= 1;
        }
    }

    fn move_right(&mut self) {
        let len = self.input.chars().count();
        if self.input_cursor < len {
            self.input_cursor += 1;
        }
    }

    fn move_line_start(&mut self) {
        let (line_start, _, _, _) = line_bounds_and_col(&self.input, self.input_cursor);
        self.input_cursor = line_start;
    }

    fn move_line_end(&mut self) {
        let (_, line_end, _, _) = line_bounds_and_col(&self.input, self.input_cursor);
        self.input_cursor = line_end;
    }

    fn move_up(&mut self) {
        let (line_start, _, col, _) = line_bounds_and_col(&self.input, self.input_cursor);
        if line_start == 0 {
            return;
        }
        let prev_end = line_start - 1;
        let prev_start = line_start_for_idx(&self.input, prev_end);
        self.input_cursor = (prev_start + col).min(prev_end);
    }

    fn move_down(&mut self) {
        let (_, line_end, col, len) = line_bounds_and_col(&self.input, self.input_cursor);
        if line_end >= len {
            return;
        }
        let next_start = line_end + 1;
        let next_end = line_end_for_idx(&self.input, next_start);
        self.input_cursor = (next_start + col).min(next_end);
    }

    fn move_word_left(&mut self) {
        self.input_cursor = word_left_boundary(&self.input, self.input_cursor);
    }

    fn move_word_right(&mut self) {
        self.input_cursor = word_right_boundary(&self.input, self.input_cursor);
    }

    fn delete_word_left(&mut self) {
        if self.input_cursor == 0 {
            return;
        }
        let start = word_left_boundary(&self.input, self.input_cursor);
        let start_byte = char_to_byte_idx(&self.input, start);
        let end_byte = char_to_byte_idx(&self.input, self.input_cursor);
        self.input.replace_range(start_byte..end_byte, "");
        self.input_cursor = start;
    }

    fn delete_word_right(&mut self) {
        let len = self.input.chars().count();
        if self.input_cursor >= len {
            return;
        }
        let end = word_right_boundary(&self.input, self.input_cursor);
        let start_byte = char_to_byte_idx(&self.input, self.input_cursor);
        let end_byte = char_to_byte_idx(&self.input, end);
        self.input.replace_range(start_byte..end_byte, "");
    }
}

fn tui_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut ReplApp,
) -> Result<(), String> {
    loop {
        app.drain_output();
        draw_ui(terminal, app)?;
        if app.should_quit {
            break;
        }

        if !event::poll(Duration::from_millis(40)).map_err(|e| format!("Event poll failed: {e}"))? {
            continue;
        }

        let ev = event::read().map_err(|e| format!("Event read failed: {e}"))?;
        if let Event::Key(key) = ev {
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match (key.code, key.modifiers) {
                (KeyCode::Char('d'), m) if m.contains(KeyModifiers::CONTROL) => {
                    app.should_quit = true
                }
                (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => {
                    app.input.clear();
                    app.input_cursor = 0;
                }
                (KeyCode::Char('f'), m) if m.contains(KeyModifiers::CONTROL) => app.move_right(),
                (KeyCode::Char('b'), m) if m.contains(KeyModifiers::CONTROL) => app.move_left(),
                (KeyCode::Char('a'), m) if m.contains(KeyModifiers::CONTROL) => {
                    app.move_line_start()
                }
                (KeyCode::Char('e'), m) if m.contains(KeyModifiers::CONTROL) => app.move_line_end(),
                (KeyCode::Char('p'), m) if m.contains(KeyModifiers::CONTROL) => app.move_up(),
                (KeyCode::Char('n'), m) if m.contains(KeyModifiers::CONTROL) => app.move_down(),
                (KeyCode::Char('f'), m) if m.contains(KeyModifiers::ALT) => app.move_word_right(),
                (KeyCode::Char('b'), m) if m.contains(KeyModifiers::ALT) => app.move_word_left(),
                (KeyCode::Char('d'), m) if m.contains(KeyModifiers::ALT) => app.delete_word_right(),
                (KeyCode::Char('r'), m) if m.contains(KeyModifiers::CONTROL) => app.submit(),
                (KeyCode::Char('l'), m) if m.contains(KeyModifiers::CONTROL) => {
                    app.entries.clear();
                    app.output_scroll = 0;
                    app.follow_output = true;
                }
                (KeyCode::Enter, m) if m.contains(KeyModifiers::CONTROL) => app.submit(),
                (KeyCode::Enter, _) => {
                    if should_submit_on_enter(&app.input) {
                        app.submit();
                    } else {
                        app.insert_str_at_cursor("\n");
                    }
                }
                (KeyCode::F(5), _) => app.submit(),
                (KeyCode::Backspace, m) if m.contains(KeyModifiers::ALT) => app.delete_word_left(),
                (KeyCode::Backspace, _) => app.backspace_at_cursor(),
                (KeyCode::Delete, _) => app.delete_at_cursor(),
                (KeyCode::Tab, _) => app.insert_str_at_cursor("  "),
                (KeyCode::Up, _) => app.move_up(),
                (KeyCode::Down, _) => app.move_down(),
                (KeyCode::Left, m) if m.contains(KeyModifiers::ALT) => app.move_word_left(),
                (KeyCode::Right, m) if m.contains(KeyModifiers::ALT) => app.move_word_right(),
                (KeyCode::Left, _) => app.move_left(),
                (KeyCode::Right, _) => app.move_right(),
                (KeyCode::Home, _) => app.move_line_start(),
                (KeyCode::End, _) => app.move_line_end(),
                (KeyCode::PageUp, _) => {
                    app.follow_output = false;
                    app.output_scroll = app.output_scroll.saturating_sub(6);
                }
                (KeyCode::PageDown, _) => {
                    app.follow_output = false;
                    app.output_scroll = app.output_scroll.saturating_add(6);
                }
                (KeyCode::Char(ch), _) => app.insert_str_at_cursor(&ch.to_string()),
                _ => {}
            }
        }
    }
    Ok(())
}

fn draw_ui(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut ReplApp,
) -> Result<(), String> {
    let lines = app.notebook_lines();
    let (cursor_line_in_stream, cursor_col_in_line) = notebook_cursor_position(app);

    terminal
        .draw(|f| {
            let area = f.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(10),
                    Constraint::Length(1),
                ])
                .split(area);

            let output_height = chunks[0].height as usize;
            let total_lines = lines.len();
            let max_scroll = total_lines.saturating_sub(output_height) as u16;
            let scroll = if app.follow_output {
                max_scroll
            } else {
                app.output_scroll.min(max_scroll)
            };
            if app.follow_output {
                app.output_scroll = max_scroll;
            }

            let output = Paragraph::new(lines.clone())
                .wrap(Wrap { trim: false })
                .scroll((scroll, 0));
            f.render_widget(output, chunks[0]);

            let visible_cursor_y = cursor_line_in_stream.saturating_sub(scroll as usize) as u16;
            if visible_cursor_y < chunks[0].height {
                let cursor_x = chunks[0].x + cursor_col_in_line;
                let cursor_y = chunks[0].y + visible_cursor_y;
                f.set_cursor_position((cursor_x, cursor_y));
            }

            let status = Paragraph::new(
                "Enter run/newline | arrows + Ctrl+f/b/a/e/p/n + Alt+f/b/left/right move | Alt+Backspace/Alt+d word delete | Ctrl+R run | Ctrl+D quit",
            );
            f.render_widget(status, chunks[1]);
        })
        .map_err(|e| format!("Draw failed: {e}"))?;

    Ok(())
}

fn notebook_cursor_position(app: &ReplApp) -> (usize, u16) {
    let mut line_idx = 0usize;
    for entry in &app.entries {
        let mut count = entry.text.lines().count();
        if count == 0 {
            count = 1;
        }
        line_idx += count;
    }

    let prefix = format!("In [{}]: ", app.next_exec_id);
    let (_, _, col, line_offset) = line_bounds_and_col(&app.input, app.input_cursor);
    (
        line_idx + line_offset,
        (prefix.chars().count() + col) as u16,
    )
}

fn char_to_byte_idx(s: &str, char_idx: usize) -> usize {
    if char_idx == 0 {
        return 0;
    }
    let mut count = 0usize;
    for (byte_idx, _) in s.char_indices() {
        if count == char_idx {
            return byte_idx;
        }
        count += 1;
    }
    s.len()
}

fn line_start_for_idx(s: &str, idx: usize) -> usize {
    let chars: Vec<char> = s.chars().collect();
    let mut i = idx.min(chars.len());
    while i > 0 {
        if chars[i - 1] == '\n' {
            break;
        }
        i -= 1;
    }
    i
}

fn line_end_for_idx(s: &str, idx: usize) -> usize {
    let chars: Vec<char> = s.chars().collect();
    let mut i = idx.min(chars.len());
    while i < chars.len() && chars[i] != '\n' {
        i += 1;
    }
    i
}

fn line_bounds_and_col(s: &str, idx: usize) -> (usize, usize, usize, usize) {
    let chars: Vec<char> = s.chars().collect();
    let clamped = idx.min(chars.len());
    let start = line_start_for_idx(s, clamped);
    let end = line_end_for_idx(s, clamped);
    let col = clamped.saturating_sub(start);
    let mut line_no = 0usize;
    for ch in chars.iter().take(start) {
        if *ch == '\n' {
            line_no += 1;
        }
    }
    (start, end, col, line_no)
}

fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn word_left_boundary(s: &str, idx: usize) -> usize {
    let chars: Vec<char> = s.chars().collect();
    if chars.is_empty() {
        return 0;
    }
    let mut i = idx.min(chars.len());
    if i == 0 {
        return 0;
    }

    while i > 0 && chars[i - 1].is_whitespace() {
        i -= 1;
    }
    if i == 0 {
        return 0;
    }

    let class = is_word_char(chars[i - 1]);
    while i > 0 && !chars[i - 1].is_whitespace() && is_word_char(chars[i - 1]) == class {
        i -= 1;
    }
    i
}

fn word_right_boundary(s: &str, idx: usize) -> usize {
    let chars: Vec<char> = s.chars().collect();
    if chars.is_empty() {
        return 0;
    }
    let mut i = idx.min(chars.len());
    if i >= chars.len() {
        return chars.len();
    }

    while i < chars.len() && chars[i].is_whitespace() {
        i += 1;
    }
    if i >= chars.len() {
        return chars.len();
    }

    let class = is_word_char(chars[i]);
    while i < chars.len() && !chars[i].is_whitespace() && is_word_char(chars[i]) == class {
        i += 1;
    }
    i
}

fn should_submit_on_enter(source: &str) -> bool {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.starts_with(':') {
        return true;
    }
    !source_is_incomplete(trimmed)
}

fn source_is_incomplete(source: &str) -> bool {
    let mut scanner = Scanner::new(source);
    let tokens = match scanner.scan_tokens() {
        Ok(v) => v,
        Err(err) => return err.contains("Unterminated string"),
    };
    let mut parser = Parser::new(tokens, source, "<repl>".to_string());
    match parser.parse_program() {
        Ok(_) => false,
        Err(err) => {
            err.contains("(got Eof)")
                || err.contains("Expected '}' after block")
                || err.contains("Expected ')' after")
                || err.contains("Expected ']' after")
                || err.contains("Unbalanced braces in shell block")
        }
    }
}

fn run_line_repl() -> Result<(), String> {
    let pending_output = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut executor = Executor::new("<repl>".to_string(), "".to_string());
    register_builtins(&mut executor);

    {
        let output = pending_output.clone();
        executor.set_output_sink(Some(Arc::new(move |line: String| {
            if let Ok(mut q) = output.lock() {
                q.push(line);
            }
        })));
    }

    let history_path = history_path();
    let mut history = load_history(&history_path).unwrap_or_default();
    let mut timing_enabled = false;
    let mut buffer = String::new();

    println!("v2r repl (line mode)");
    println!(
        "commands: :help :quit :clear :vars :load <file> :ast <code> :tokens <code> :time <on|off> :history export <path> :history import <path>"
    );

    loop {
        let prompt = if buffer.is_empty() { "v2r> " } else { "...> " };
        print!("{prompt}");
        io::stdout()
            .flush()
            .map_err(|e| format!("Failed to flush stdout: {e}"))?;

        let mut line = String::new();
        let read = io::stdin()
            .read_line(&mut line)
            .map_err(|e| format!("Failed to read input: {e}"))?;
        if read == 0 {
            break;
        }

        if buffer.is_empty() && line.trim().is_empty() {
            continue;
        }

        buffer.push_str(&line);
        if line.trim_end().ends_with('\\') {
            continue;
        }

        let src = buffer.trim_end().to_string();
        buffer.clear();
        if src.is_empty() {
            continue;
        }

        if !history.last().map(|s| s == &src).unwrap_or(false) {
            history.push(src.clone());
        }

        if src.starts_with(':') {
            if line_mode_command(
                &mut executor,
                &pending_output,
                &mut history,
                &mut timing_enabled,
                &src,
            )? {
                break;
            }
            line_mode_drain_output(&pending_output);
            continue;
        }

        line_mode_eval(
            &mut executor,
            &pending_output,
            "<repl>".to_string(),
            src,
            true,
            timing_enabled,
        );
    }

    save_history_to(&history_path, &history)?;
    Ok(())
}

fn line_mode_eval(
    executor: &mut Executor,
    pending_output: &Arc<Mutex<Vec<String>>>,
    filename: String,
    source: String,
    auto_print_result: bool,
    timing_enabled: bool,
) {
    let start = Instant::now();
    executor.set_context(filename.clone(), source.clone());

    let mut scanner = Scanner::new(&source);
    let tokens = match scanner.scan_tokens() {
        Ok(v) => v,
        Err(err) => {
            eprintln!("{filename}: {err}");
            return;
        }
    };

    let mut parser = Parser::new(tokens, &source, filename.clone());
    let program = match parser.parse_program() {
        Ok(v) => v,
        Err(err) => {
            eprintln!("{err}");
            return;
        }
    };

    match executor.execute_repl(&program) {
        Ok(last) => {
            line_mode_drain_output(pending_output);
            if auto_print_result {
                if let Some(v) = last {
                    println!("{}", v.as_string());
                }
            }
            if timing_enabled {
                println!("time: {} ms", start.elapsed().as_millis());
            }
        }
        Err(err) => {
            line_mode_drain_output(pending_output);
            eprintln!("{err}");
        }
    }
}

fn line_mode_command(
    executor: &mut Executor,
    pending_output: &Arc<Mutex<Vec<String>>>,
    history: &mut Vec<String>,
    timing_enabled: &mut bool,
    line: &str,
) -> Result<bool, String> {
    let rest = line.trim_start_matches(':').trim();
    let mut parts = rest.splitn(2, char::is_whitespace);
    let cmd = parts.next().unwrap_or("");
    let arg = parts.next().unwrap_or("").trim();

    match cmd {
        "help" => {
            println!(
                ":help :quit :clear :vars :load <file> :ast <code> :tokens <code> :time <on|off> :history export <path> :history import <path>"
            );
        }
        "quit" | "q" | "exit" => return Ok(true),
        "clear" => {
            print!("\x1B[2J\x1B[1;1H");
            io::stdout()
                .flush()
                .map_err(|e| format!("Failed to flush stdout: {e}"))?;
        }
        "vars" => {
            let vars = executor.list_globals();
            if vars.is_empty() {
                println!("No user globals");
            } else {
                for (name, value) in vars {
                    println!("{name} = {}", value.as_string());
                }
            }
        }
        "load" => {
            if arg.is_empty() {
                eprintln!("Usage: :load <file>");
            } else {
                match fs::read_to_string(arg) {
                    Ok(src) => line_mode_eval(
                        executor,
                        pending_output,
                        arg.to_string(),
                        src,
                        false,
                        *timing_enabled,
                    ),
                    Err(err) => eprintln!("Failed to read {arg}: {err}"),
                }
            }
        }
        "ast" => {
            if arg.is_empty() {
                eprintln!("Usage: :ast <code>");
            } else {
                let mut scanner = Scanner::new(arg);
                let tokens = scanner.scan_tokens().map_err(|e| format!("<repl>: {e}"))?;
                let mut parser = Parser::new(tokens, arg, "<repl>".to_string());
                match parser.parse_program() {
                    Ok(program) => println!("{program:#?}"),
                    Err(err) => eprintln!("{err}"),
                }
            }
        }
        "tokens" => {
            if arg.is_empty() {
                eprintln!("Usage: :tokens <code>");
            } else {
                let mut scanner = Scanner::new(arg);
                match scanner.scan_tokens() {
                    Ok(tokens) => {
                        for t in tokens {
                            println!("{}:{} {:?}", t.line, t.column, t.kind);
                        }
                    }
                    Err(err) => eprintln!("<repl>: {err}"),
                }
            }
        }
        "time" => {
            if arg == "on" {
                *timing_enabled = true;
                println!("Timing enabled");
            } else if arg == "off" {
                *timing_enabled = false;
                println!("Timing disabled");
            } else {
                eprintln!("Usage: :time <on|off>");
            }
        }
        "history" => {
            let mut hparts = arg.splitn(2, char::is_whitespace);
            let sub = hparts.next().unwrap_or("");
            let path = hparts.next().unwrap_or("").trim();
            match sub {
                "export" => {
                    if path.is_empty() {
                        eprintln!("Usage: :history export <path>");
                    } else {
                        save_history_to(path, history)?;
                        println!("History exported to {path}");
                    }
                }
                "import" => {
                    if path.is_empty() {
                        eprintln!("Usage: :history import <path>");
                    } else {
                        *history = load_history(Path::new(path))?;
                        println!("Imported {} history entries", history.len());
                    }
                }
                _ => eprintln!("Usage: :history <export|import> <path>"),
            }
        }
        _ => eprintln!("Unknown command: :{cmd}"),
    }

    Ok(false)
}

fn line_mode_drain_output(pending_output: &Arc<Mutex<Vec<String>>>) {
    let lines = if let Ok(mut out) = pending_output.lock() {
        std::mem::take(&mut *out)
    } else {
        Vec::new()
    };
    for line in lines {
        println!("{line}");
    }
}

fn history_path() -> PathBuf {
    if let Ok(cwd) = std::env::current_dir() {
        return cwd.join(".v2r_repl_history");
    }
    PathBuf::from(".v2r_repl_history")
}

fn load_history(path: &Path) -> Result<Vec<String>, String> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read history {}: {e}", path.display()))?;
    Ok(data
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect())
}

fn save_history_to<P: AsRef<Path>>(path: P, history: &[String]) -> Result<(), String> {
    let path = path.as_ref();
    let body = history.join("\n");
    fs::write(path, body).map_err(|e| format!("Failed to write history {}: {e}", path.display()))
}
