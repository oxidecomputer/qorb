use clap::Parser;
use std::io::stdout;

use crossterm::{
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use futures::stream::StreamExt;
use ratatui::{prelude::*, widgets::*};
use tokio_tungstenite::tungstenite;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    #[clap(long, short = 'i', default_value_t = 2)]
    pub update_interval_secs: u8,

    /// qtop stats URL to connect to
    pub url: http::Uri,
}

#[derive(serde::Deserialize, Debug)]
struct Update {
    claims: usize,
    sets: std::collections::BTreeMap<String, SetStats>,
}

#[derive(serde::Deserialize, Debug)]
struct SetStats {
    connecting_slots: usize,
    unclaimed_slots: usize,
    checking_slots: usize,
    claimed_slots: usize,
}

impl Update {
    fn render(self, frame: &mut Frame, uri: &http::Uri) {
        let main_layout = Layout::new(
            Direction::Vertical,
            [
                Constraint::Length(1),
                Constraint::Min(1),
                Constraint::Length(1),
            ],
        )
        .split(frame.size());
        const NAME: &str = "NAME";
        const CONNECTING: &str = "CONNECTING";
        const UNCLAIMED: &str = "UNCLAIMED";
        const CLAIMED: &str = "CLAIMED";
        const CHECKING: &str = "CHECKING";
        let longest_name = self.sets.keys().map(String::len).max().unwrap_or(0);
        let rows = self.sets.iter().map(|(name, stats)| {
            Row::new(vec![
                Cell::from(name.clone()),
                Cell::from(
                    Text::from(stats.connecting_slots.to_string()).alignment(Alignment::Right),
                ),
                Cell::from(
                    Text::from(stats.unclaimed_slots.to_string()).alignment(Alignment::Right),
                ),
                Cell::from(
                    Text::from(stats.checking_slots.to_string()).alignment(Alignment::Right),
                ),
                Cell::from(Text::from(stats.claimed_slots.to_string()).alignment(Alignment::Right)),
            ])
        });
        let table = Table::new(
            rows,
            [
                Constraint::Min(longest_name as u16),
                Constraint::Length(CONNECTING.len() as u16),
                Constraint::Length(UNCLAIMED.len() as u16),
                Constraint::Length(CHECKING.len() as u16),
                Constraint::Length(CLAIMED.len() as u16),
            ],
        )
        .block(Block::default().borders(Borders::ALL).title(
            format!("QTOP - {uri} - {} claims/s", self.claims).set_style(Style::default().bold()),
        ))
        .header(
            Row::new(vec![
                Cell::from(NAME),
                Cell::from(CONNECTING),
                Cell::from(UNCLAIMED),
                Cell::from(CHECKING),
                Cell::from(CLAIMED),
            ])
            .set_style(Style::default().bold()),
        );
        frame.render_widget(table, main_layout[1]);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = Args::parse();
    let (mut ws, _) = tokio_tungstenite::connect_async(args.url.clone()).await?;
    stdout().execute(EnterAlternateScreen)?;

    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

    while let Some(wsmsg) = ws.next().await {
        let stats: Update = match wsmsg? {
            tungstenite::Message::Text(txt) => serde_json::from_str(&txt)?,
            tungstenite::Message::Binary(bin) => serde_json::from_slice(&bin)?,
            tungstenite::Message::Close(_) => {
                eprintln!("connection closed");
                break;
            }
            _ => continue,
        };
        terminal.draw(|f| stats.render(f, &args.url))?;
    }

    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}
