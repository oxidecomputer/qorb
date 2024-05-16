use std::io::{self, stdout};

use async_trait::async_trait;
use crossterm::{
    event::{self, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use qorb::backend;
use qorb::resolver::{self, Resolver};
use ratatui::{prelude::*, widgets::*};
use std::collections::BTreeSet;

//
// USER INPUT
//

enum Adding {
    On,
    Off,
}

impl Default for Adding {
    fn default() -> Self {
        Adding::On
    }
}

enum KeyReaction {
    Nothing,
    Quit,
    AddChar(char),
    RemoveChar,
    SubmitBackend,
    ToggleAddMode,
}

fn poll_keys() -> io::Result<KeyReaction> {
    if event::poll(std::time::Duration::from_millis(50))? {
        if let Event::Key(key) = event::read()? {
            if key.kind != event::KeyEventKind::Press {
                return Ok(KeyReaction::Nothing);
            }

            if key.modifiers == event::KeyModifiers::CONTROL {
                match key.code {
                    KeyCode::Char('c') | KeyCode::Char('q') => return Ok(KeyReaction::Quit),
                    _ => return Ok(KeyReaction::Nothing),
                }
            }
            match key.code {
                KeyCode::Tab => return Ok(KeyReaction::ToggleAddMode),
                KeyCode::Char(c) => return Ok(KeyReaction::AddChar(c)),
                KeyCode::Backspace => return Ok(KeyReaction::RemoveChar),
                KeyCode::Enter => return Ok(KeyReaction::SubmitBackend),
                _ => (),
            }
        }
    }
    Ok(KeyReaction::Nothing)
}

//
// RENDERING THE UI
//

fn ui(frame: &mut Frame, app: &App) {
    let main_layout = Layout::new(
        Direction::Vertical,
        [
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ],
    )
    .split(frame.size());

    // The list of resolver entries
    let mut list_items = vec![];
    for backend in &app.backends {
        list_items.push(Text::styled(
            backend.address.to_string(),
            Style::default().cyan(),
        ))
    }
    for backend in &app.added_backends {
        list_items.push(Text::styled(
            backend.address.to_string(),
            Style::default().green(),
        ))
    }
    for backend in &app.removed_backends {
        list_items.push(Text::styled(
            backend.address.to_string(),
            Style::default().red(),
        ))
    }
    let backends_list = List::new(list_items);

    // The help box for indicating what the user should type
    let lines = vec![
        Line::from(vec![
            "Press ".into(),
            "ENTER".bold(),
            " to submit a backend".into(),
        ])
        .centered(),
        Line::from(vec![
            "Press ".into(),
            "TAB".bold(),
            " to toggle backend addition / removal".into(),
        ])
        .centered(),
        Line::from(vec!["Press ".into(), "CTRL + Q".bold(), " to quit".into()]).centered(),
    ];
    let style = Style::default();
    let text = Text::from(lines).patch_style(style);
    let help_message = Paragraph::new(text);

    // Area where a user should type
    let (user_input_line, style) = if let Some(feedback) = &app.feedback {
        (
            Line::from(vec![" !! ".bold(), feedback.into()]),
            Style::default().fg(Color::Red),
        )
    } else {
        match app.adding {
            Adding::On => (
                Line::from(vec![" ++ ".bold(), app.user_input_buffer.clone().into()]),
                Style::default().fg(Color::Green),
            ),
            Adding::Off => (
                Line::from(vec![" -- ".bold(), app.user_input_buffer.clone().into()]),
                Style::default().fg(Color::LightRed),
            ),
        }
    };
    let user_input_line_length = user_input_line.width();
    let text = Text::from(user_input_line).patch_style(style);
    let user_input = Paragraph::new(text);

    let inner_layout = Layout::new(
        Direction::Vertical,
        [
            Constraint::Percentage(100),
            Constraint::Length(5),
            Constraint::Length(3),
        ],
    )
    .split(main_layout[1]);

    frame.render_widget(
        backends_list.block(Block::default().borders(Borders::ALL).title("Backends")),
        inner_layout[0],
    );
    frame.render_widget(
        help_message.block(Block::default().borders(Borders::ALL)),
        inner_layout[1],
    );
    frame.render_widget(
        user_input.block(Block::default().borders(Borders::ALL)),
        inner_layout[2],
    );
    frame.set_cursor(
        inner_layout[2].x + 1 + user_input_line_length as u16,
        inner_layout[2].y + 1,
    );
}

//
// APPLICATION LOGIC
//

#[derive(Default)]
struct App {
    user_input_buffer: String,

    feedback: Option<String>,

    adding: Adding,

    // This is a "manual resolver", where each backend is only identified by the
    // address it contains.
    backends: BTreeSet<backend::Backend>,

    added_backends: BTreeSet<backend::Backend>,

    removed_backends: BTreeSet<backend::Backend>,
}

impl App {
    fn submit_resolver(&mut self) {
        if self.user_input_buffer.is_empty() {
            // Allow 'enter' to clear the screen without causing more errors.
            return;
        }

        let addr: Result<std::net::SocketAddr, _> = self.user_input_buffer.parse();
        self.user_input_buffer.clear();

        let Ok(address) = addr else {
            self.feedback = Some(
                "Could not parse address as SocketAddr. \
                Try a format like: 127.0.0.1:1234 or [::1]:1234"
                    .to_string(),
            );
            return;
        };

        let backend = backend::Backend { address };

        match self.adding {
            Adding::On => {
                if self.backends.contains(&backend) {
                    self.feedback = Some("Already added".to_string());
                    return;
                }
                if self.added_backends.contains(&backend) {
                    self.feedback = Some("Already in the process of being added".to_string());
                    return;
                }
                if self.removed_backends.contains(&backend) {
                    self.feedback =
                        Some("Wait a sec for the backend to be removed first".to_string());
                    return;
                }

                self.added_backends.insert(backend);
            }
            Adding::Off => {
                if self.removed_backends.contains(&backend) {
                    self.feedback = Some("Backend already being removed".to_string());
                    return;
                }

                if self.added_backends.contains(&backend) {
                    self.added_backends.remove(&backend);
                    return;
                }
                self.backends.remove(&backend);
            }
        }
    }
}

#[async_trait]
impl Resolver for App {
    async fn step(&mut self) -> Vec<resolver::Event> {
        let mut events = vec![];
        if !self.added_backends.is_empty() {
            events.push(resolver::Event::Added(
                self.added_backends
                    .iter()
                    .map(|backend| (backend::Name(backend.address.to_string()), backend.clone()))
                    .collect(),
            ));
        }
        for b in &self.added_backends {
            self.backends.insert(b.clone());
        }
        self.added_backends.clear();

        if !self.removed_backends.is_empty() {
            events.push(resolver::Event::Removed(
                self.removed_backends
                    .iter()
                    .map(|backend| backend::Name(backend.address.to_string()))
                    .collect(),
            ));
        }
        for b in &self.removed_backends {
            self.backends.remove(&b);
        }
        self.removed_backends.clear();
        events
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

    let mut app = App::default();
    loop {
        terminal.draw(|f| ui(f, &app))?;

        let action = poll_keys()?;

        match action {
            KeyReaction::Quit => break,
            KeyReaction::Nothing => (),
            KeyReaction::AddChar(c) => {
                app.feedback.take();
                app.user_input_buffer.push(c);
            }
            KeyReaction::RemoveChar => {
                app.feedback.take();
                let _ = app.user_input_buffer.pop();
            }
            KeyReaction::ToggleAddMode => {
                app.feedback.take();
                match app.adding {
                    Adding::On => app.adding = Adding::Off,
                    Adding::Off => app.adding = Adding::On,
                }
            }
            KeyReaction::SubmitBackend => {
                app.feedback.take();
                app.submit_resolver();
            }
        }
    }

    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}
