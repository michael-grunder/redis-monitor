use std::{
    io::{self, IsTerminal, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use crossterm::event::{
    self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
};
use tokio::sync::watch;

use crate::filter::{Filter, FilterPattern};

const INPUT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const CLEAR_LINE: &[u8] = b"\r\x1b[2K";

#[derive(Debug, Default)]
struct DisplayState {
    prompt: Option<String>,
    output_active: bool,
}

#[derive(Debug, Default)]
pub struct TerminalUi {
    state: Mutex<DisplayState>,
    display_failed: AtomicBool,
}

pub struct OutputGuard<'a> {
    ui: &'a TerminalUi,
}

pub struct TerminalInput {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    ui: Arc<TerminalUi>,
}

#[derive(Debug, Default)]
struct Editor {
    current: String,
    draft: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
enum EditorAction {
    None,
    Redraw,
    Submit(String),
    Cancel,
    Shutdown,
}

#[cfg(unix)]
struct RawMode {
    original: rustix::termios::Termios,
}

#[cfg(not(unix))]
struct RawMode;

impl RawMode {
    #[cfg(unix)]
    fn enable() -> io::Result<Self> {
        use rustix::{
            stdio::stdin,
            termios::{OptionalActions, tcgetattr, tcsetattr},
        };

        let original = tcgetattr(stdin())?;
        let mut editing = original.clone();
        let output_modes = editing.output_modes;
        editing.make_raw();
        // The monitor continues writing ordinary newline-delimited output
        // while input is edited, so preserve its terminal output processing.
        editing.output_modes = output_modes;
        tcsetattr(stdin(), OptionalActions::Now, &editing)?;
        Ok(Self { original })
    }

    #[cfg(not(unix))]
    fn enable() -> io::Result<Self> {
        crossterm::terminal::enable_raw_mode()?;
        Ok(Self)
    }
}

#[cfg(unix)]
impl Drop for RawMode {
    fn drop(&mut self) {
        if let Err(error) = rustix::termios::tcsetattr(
            rustix::stdio::stdin(),
            rustix::termios::OptionalActions::Now,
            &self.original,
        ) {
            eprintln!("Failed to restore terminal input mode: {error}");
        }
    }
}

#[cfg(not(unix))]
impl Drop for RawMode {
    fn drop(&mut self) {
        if let Err(error) = crossterm::terminal::disable_raw_mode() {
            eprintln!("Failed to restore terminal input mode: {error}");
        }
    }
}

impl TerminalUi {
    fn draw(&self, message: Option<&str>, prompt: Option<&str>) {
        if self.display_failed.load(Ordering::Relaxed) {
            return;
        }

        let result = (|| {
            let mut stderr = io::stderr().lock();
            stderr.write_all(CLEAR_LINE)?;
            if let Some(message) = message {
                writeln!(stderr, "{message}")?;
            }
            if let Some(prompt) = prompt {
                stderr.write_all(b"/")?;
                stderr.write_all(prompt.as_bytes())?;
            }
            stderr.flush()
        })();

        // Prompt rendering is advisory. A closed stderr must not stop record
        // processing or prevent an already-entered filter from being applied.
        if result.is_err() {
            self.display_failed.store(true, Ordering::Relaxed);
        }
    }

    fn render(&self, state: &DisplayState) {
        if state.output_active {
            return;
        }

        let Some(prompt) = &state.prompt else {
            return;
        };

        self.draw(None, Some(prompt));
    }

    fn set_prompt(&self, prompt: Option<&str>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let clear_existing =
            state.prompt.is_some() && prompt.is_none() && !state.output_active;
        state.prompt = prompt.map(str::to_owned);
        if clear_existing {
            self.draw(None, None);
        } else {
            self.render(&state);
        }
        drop(state);
    }

    fn message(&self, message: &str) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.output_active {
            return;
        }

        self.draw(Some(message), state.prompt.as_deref());
        drop(state);
    }

    pub fn begin_output(&self) -> OutputGuard<'_> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.prompt.is_some() {
            self.draw(None, None);
        }
        state.output_active = true;
        drop(state);
        OutputGuard { ui: self }
    }
}

impl Drop for OutputGuard<'_> {
    fn drop(&mut self) {
        let mut state = self
            .ui
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.output_active = false;
        self.ui.render(&state);
        drop(state);
    }
}

impl Editor {
    fn handle_key(&mut self, key: KeyEvent) -> EditorAction {
        if key.kind == KeyEventKind::Release {
            return EditorAction::None;
        }

        if key.modifiers.contains(KeyModifiers::CONTROL)
            && key.code == KeyCode::Char('c')
        {
            return EditorAction::Shutdown;
        }

        let Some(draft) = self.draft.as_mut() else {
            if key.code == KeyCode::Char('/') {
                self.draft = Some(self.current.clone());
                return EditorAction::Redraw;
            }
            return EditorAction::None;
        };

        match key.code {
            KeyCode::Enter => EditorAction::Submit(draft.clone()),
            KeyCode::Esc => {
                self.draft = None;
                EditorAction::Cancel
            }
            KeyCode::Backspace => {
                draft.pop();
                EditorAction::Redraw
            }
            KeyCode::Char(ch)
                if key.modifiers.is_empty()
                    || key.modifiers == KeyModifiers::SHIFT =>
            {
                draft.push(ch);
                EditorAction::Redraw
            }
            _ => EditorAction::None,
        }
    }

    fn commit(&mut self) {
        if let Some(draft) = self.draft.take() {
            self.current = draft;
        }
    }
}

fn compile_filter(raw: &str) -> anyhow::Result<Filter> {
    let patterns = if raw.is_empty() {
        Vec::new()
    } else {
        vec![raw.parse::<FilterPattern>()?]
    };
    Filter::new(patterns)
}

fn run_input(
    stop: &AtomicBool,
    filters: &watch::Sender<Filter>,
    shutdown: &watch::Sender<bool>,
    ui: &TerminalUi,
) {
    let mut editor = Editor::default();

    while !stop.load(Ordering::Relaxed) {
        match event::poll(INPUT_POLL_INTERVAL) {
            Ok(false) => continue,
            Err(error) => {
                ui.message(&format!("Interactive input error: {error}"));
                break;
            }
            Ok(true) => {}
        }

        let event = match event::read() {
            Ok(event) => event,
            Err(error) => {
                ui.message(&format!("Interactive input error: {error}"));
                break;
            }
        };
        let Event::Key(key) = event else { continue };

        match editor.handle_key(key) {
            EditorAction::None => {}
            EditorAction::Redraw => ui.set_prompt(editor.draft.as_deref()),
            EditorAction::Cancel => ui.set_prompt(None),
            EditorAction::Shutdown => {
                ui.set_prompt(None);
                // A missing receiver means the runtime is already finishing.
                let _ = shutdown.send(true);
                break;
            }
            EditorAction::Submit(raw) => match compile_filter(&raw) {
                Ok(filter) => {
                    if filters.send(filter).is_err() {
                        break;
                    }
                    editor.commit();
                    ui.set_prompt(None);
                    if raw.is_empty() {
                        ui.message("Interactive filter cleared");
                    } else {
                        ui.message(&format!("Interactive filter: {raw}"));
                    }
                }
                Err(error) => {
                    ui.message(&format!("Invalid filter: {error}"));
                }
            },
        }
    }

    ui.set_prompt(None);
}

impl TerminalInput {
    pub fn start(
        filters: watch::Sender<Filter>,
        shutdown: watch::Sender<bool>,
    ) -> io::Result<Option<Self>> {
        if !io::stdin().is_terminal() || !io::stderr().is_terminal() {
            return Ok(None);
        }

        let raw_mode = RawMode::enable()?;
        let stop = Arc::new(AtomicBool::new(false));
        let ui = Arc::new(TerminalUi::default());
        let thread_stop = Arc::clone(&stop);
        let thread_ui = Arc::clone(&ui);
        let thread = thread::Builder::new()
            .name("interactive-input".to_string())
            .spawn(move || {
                let _raw_mode = raw_mode;
                run_input(&thread_stop, &filters, &shutdown, &thread_ui);
            })?;

        Ok(Some(Self {
            stop,
            thread: Some(thread),
            ui,
        }))
    }

    pub fn ui(&self) -> Arc<TerminalUi> {
        Arc::clone(&self.ui)
    }
}

impl Drop for TerminalInput {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            eprintln!("Interactive input thread panicked");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn slash_edits_the_current_filter_and_backspace_can_clear_it() {
        let mut editor = Editor {
            current: "get".to_string(),
            draft: None,
        };

        assert_eq!(
            editor.handle_key(key(KeyCode::Char('/'))),
            EditorAction::Redraw
        );
        assert_eq!(editor.draft.as_deref(), Some("get"));
        for _ in 0..3 {
            assert_eq!(
                editor.handle_key(key(KeyCode::Backspace)),
                EditorAction::Redraw
            );
        }
        assert_eq!(
            editor.handle_key(key(KeyCode::Enter)),
            EditorAction::Submit(String::new())
        );
    }

    #[test]
    fn live_filter_uses_existing_negative_filter_syntax() {
        let filter = compile_filter("!get").unwrap();

        assert!(!filter.matches(b"GET"));
        assert!(filter.matches(b"SET"));
    }

    #[test]
    fn live_filter_uses_existing_regex_filter_syntax() {
        let filter = compile_filter("/^geo/").unwrap();

        assert!(filter.matches(b"geoadd"));
        assert!(!filter.matches(b"get"));
        assert!(compile_filter("/[/").is_err());
    }

    #[test]
    fn backspace_removes_a_complete_unicode_character() {
        let mut editor = Editor {
            current: "gé".to_string(),
            draft: Some("gé".to_string()),
        };

        assert_eq!(
            editor.handle_key(key(KeyCode::Backspace)),
            EditorAction::Redraw
        );
        assert_eq!(editor.draft.as_deref(), Some("g"));
    }
}
