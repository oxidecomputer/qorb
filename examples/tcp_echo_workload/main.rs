use async_trait::async_trait;
use camino::Utf8PathBuf;
use crossterm::{
    event::{Event, EventStream, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use futures::stream::StreamExt;
use futures::FutureExt;
use qorb::backend;
use qorb::policy::{Policy, SetConfig};
use qorb::pool::Pool;
use qorb::resolvers::dns::{DnsResolver, DnsResolverConfig};
use qorb::service;
use ratatui::style::Styled;
use ratatui::{prelude::*, widgets::*};
use std::env;
use std::io::stdout;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration};

// This is almost identical to [qorb::connectors::tcp::TcpConnector], but it
// actually has an "is_valid" implementation.
struct TcpConnector {}

#[async_trait]
impl backend::Connector for TcpConnector {
    type Connection = TcpStream;

    async fn connect(
        &self,
        backend: &backend::Backend,
    ) -> Result<Self::Connection, backend::Error> {
        TcpStream::connect(backend.address)
            .await
            .map_err(|e| e.into())
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), backend::Error> {
        // It would normally be disruptive to write to an arbitrary server, but
        // since this is an echo server, it's cool for the pool to send and
        // recieve messages.
        //
        // TODO: This is a bit hacky -- do we need an explicit "reset
        // connection" call?
        let mut buf = [0; 1];
        conn.write_all(b"a").await?;
        conn.read_exact(&mut buf).await?;
        if &buf[..] != b"a" {
            return Err(backend::Error::Other(anyhow::anyhow!("bad response")));
        }
        Ok(())
    }
}

enum Tracing {
    Off,
    On,
    ReallyOn,
}

fn trace_path() -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(std::env::temp_dir().join("qorb-workload-trace")).unwrap()
}

#[cfg(feature = "qtop")]
fn log_path() -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(std::env::temp_dir().join("qorb-workload-log")).unwrap()
}

fn tracing(t: Tracing) {
    let (events, max_level) = match t {
        Tracing::Off => return,
        Tracing::On => (FmtSpan::NONE, tracing::Level::INFO),
        Tracing::ReallyOn => (FmtSpan::ENTER, tracing::Level::DEBUG),
    };

    let trace_file = std::fs::File::create(trace_path()).unwrap();

    use tracing_subscriber::fmt::format::{format, FmtSpan};
    tracing_subscriber::fmt()
        .event_format(format().compact())
        .with_thread_names(false)
        .with_span_events(events)
        .with_max_level(max_level)
        .with_writer(trace_file)
        .init();
}

fn usage(args: &[String]) {
    let trace_path = trace_path();
    eprintln!("Usage: {} <options>", args[0]);
    eprintln!("Options may include: ");
    eprintln!("  --help: See this help message");
    eprintln!("  --tracing: Enable tracing (writes to {})", trace_path);
    eprintln!("  --super-tracing: Enable more tracing");
    eprintln!("  --probes: Enable USDT DTrace probes");
    eprintln!("  --bootstrap=<DNS address>: Provide a bootstrap DNS address");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();

    let mut trace_level = Tracing::Off;
    let mut bootstrap_address = "[::1]:1234".parse()?;
    let mut enable_probes = false;
    for arg in &args[1..] {
        match arg.as_str() {
            "--tracing" => trace_level = Tracing::On,
            "--super-tracing" => trace_level = Tracing::ReallyOn,
            "--probes" => enable_probes = true,
            "--help" => {
                usage(&args);
                return Ok(());
            }
            other => match other.split_once('=') {
                Some(("--bootstrap", address)) => {
                    bootstrap_address = address.parse()?;
                }
                _ => {
                    usage(&args);
                    return Ok(());
                }
            },
        }
    }

    tracing(trace_level);
    #[cfg(feature = "probes")]
    if enable_probes {
        usdt::register_probes().unwrap();
    }

    // We need to tell the pool about at least one DNS server to get the party
    // started.
    let bootstrap_dns = vec![bootstrap_address];

    // This pool will try to lookup the echo server, using configuration
    // defined in "example/dns_server/test.com.zone".
    let resolver = Box::new(DnsResolver::new(
        service::Name("_echo._tcp.test.com.".to_string()),
        bootstrap_dns,
        DnsResolverConfig {
            query_interval: Duration::from_secs(5),
            query_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    ));
    // We're using a custom connector that lets the pool perform health
    // checks on its own.
    let backend_connector = Arc::new(TcpConnector {});

    // Tweak some of the default intervals to a smaller duration, so
    // it's easier to see what's going on.
    let policy = Policy {
        set_config: SetConfig {
            max_connection_backoff: Duration::from_secs(5),
            health_interval: Duration::from_secs(3),
            ..Default::default()
        },
        rebalance_interval: Duration::from_secs(10),
        ..Default::default()
    };

    // Actually make the pool!
    let pool = Arc::new(
        Pool::new("my-pool".to_string(), resolver, backend_connector, policy)
            .expect("USDT probe registration failed"),
    );

    #[cfg(feature = "qtop")]
    tokio::spawn(export_stats_for_qtop(pool.stats().clone()));

    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;

    // Prepare a terminal which re-draws itself
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    let mut redraw_interval = interval(Duration::from_millis(50));

    // Read input from the keyboard
    let mut reader = EventStream::new();
    let mut event_fut = reader.next().fuse();

    let mut app = AllClaimers::default();

    terminal.clear()?;

    loop {
        tokio::select! {
            Some(Ok(Event::Key(event))) = &mut event_fut => {
                event_fut = reader.next().fuse();
                match event.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Up => app.select_up(),
                    KeyCode::Down => app.select_down(),
                    KeyCode::Left => {
                        let Some(idx) = app.table_state.selected() else {
                            continue;
                        };
                        let Some(claimer) = app.claimers.get(idx) else {
                            continue;
                        };
                        let mut duration = claimer.inner.claim_duration.lock().unwrap();
                        *duration = std::cmp::max(
                            Duration::from_millis(100),
                            duration.saturating_sub(Duration::from_millis(100))
                        );
                    },
                    KeyCode::Right => {
                        let Some(idx) = app.table_state.selected() else {
                            continue;
                        };
                        let Some(claimer) = app.claimers.get(idx) else {
                            continue;
                        };
                        let mut duration = claimer.inner.claim_duration.lock().unwrap();
                        *duration = duration.saturating_add(Duration::from_millis(100));
                    },
                    KeyCode::Tab => {
                        app.claimers.push(Claimer::new(pool.clone()));
                        app.select_self();
                    }
                    KeyCode::BackTab => {
                        app.claimers.pop();
                        app.select_self();
                    }
                    _ => {},
                }
            }
            _ = redraw_interval.tick() => {
                terminal.draw(|f| app.render(f))?;
            }
        }
    }

    stdout().execute(LeaveAlternateScreen)?;
    disable_raw_mode()?;

    Ok(())
}

#[derive(Default)]
struct AllClaimers {
    table_state: TableState,
    claimers: Vec<Claimer>,
}

impl AllClaimers {
    fn select_self(&mut self) {
        let old = self.table_state.selected().unwrap_or(0);
        let new = std::cmp::min(old, self.claimers.len().saturating_sub(1));

        self.table_state.select(Some(new));
    }

    fn select_up(&mut self) {
        let old = self.table_state.selected().unwrap_or(0);
        let new = old.saturating_sub(1);
        self.table_state.select(Some(new));
    }

    fn select_down(&mut self) {
        let old = self.table_state.selected().unwrap_or(0);
        let new = std::cmp::min(old + 1, self.claimers.len().saturating_sub(1));
        self.table_state.select(Some(new));
    }

    fn render(&mut self, frame: &mut Frame) {
        let main_layout = Layout::new(
            Direction::Vertical,
            [
                Constraint::Length(1),
                Constraint::Min(1),
                Constraint::Length(1),
            ],
        )
        .split(frame.area());
        const NAME: &str = "INDEX";
        const CLAIMED: &str = "[CLAIMED]";
        const CLAIM_DURATION: &str = "[CLAIM DURATION (ms)]";
        const SUCCESSES: &str = "[SUCCESSES]";
        const FAIL_SERV: &str = "[SERVER FAILURES]";
        const FAIL_CLAIM: &str = "[CLAIM FAILURES]";

        let rows = self.claimers.iter().enumerate().map(|(idx, claimer)| {
            Row::new(vec![
                Cell::from(idx.to_string().clone()),
                Cell::from(
                    Text::from(if claimer.inner.claimed.load(Ordering::SeqCst) {
                        "✅"
                    } else {
                        ""
                    })
                    .alignment(Alignment::Right),
                ),
                Cell::from(
                    Text::from(
                        claimer
                            .inner
                            .claim_duration
                            .lock()
                            .unwrap()
                            .as_millis()
                            .to_string(),
                    )
                    .alignment(Alignment::Right),
                ),
                Cell::from(
                    Text::from(claimer.inner.count_ok.load(Ordering::SeqCst).to_string())
                        .alignment(Alignment::Right),
                ),
                Cell::from(
                    Text::from(
                        claimer
                            .inner
                            .count_err_server
                            .load(Ordering::SeqCst)
                            .to_string(),
                    )
                    .alignment(Alignment::Right),
                ),
                Cell::from(
                    Text::from(
                        claimer
                            .inner
                            .count_err_claim
                            .load(Ordering::SeqCst)
                            .to_string(),
                    )
                    .alignment(Alignment::Right),
                ),
            ])
        });
        let table = Table::new(
            rows,
            [
                Constraint::Min(CLAIM_DURATION.len() as u16),
                Constraint::Length(CLAIMED.len() as u16),
                Constraint::Length(CLAIM_DURATION.len() as u16),
                Constraint::Length(SUCCESSES.len() as u16),
                Constraint::Length(FAIL_SERV.len() as u16),
                Constraint::Length(FAIL_CLAIM.len() as u16),
            ],
        )
        .block(
            Block::default().borders(Borders::ALL).title(
                "Workload Generator"
                    .to_string()
                    .set_style(Style::default().bold()),
            ),
        )
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .highlight_spacing(HighlightSpacing::Always)
        .highlight_symbol(Text::from(">>>"))
        .header(
            Row::new(vec![
                Cell::from(NAME),
                Cell::from(Text::from(CLAIMED).alignment(Alignment::Right)),
                Cell::from(Text::from(CLAIM_DURATION).alignment(Alignment::Right)),
                Cell::from(Text::from(SUCCESSES).alignment(Alignment::Right)),
                Cell::from(Text::from(FAIL_SERV).alignment(Alignment::Right)),
                Cell::from(Text::from(FAIL_CLAIM).alignment(Alignment::Right)),
            ])
            .set_style(Style::default().bold()),
        );

        // The help box for indicating what the user should type
        let lines = vec![
            Line::from(vec![
                "Press ".into(),
                "TAB".bold(),
                " to add a task making claims, and ".into(),
                "SHIFT + TAB".bold(),
                " to remove a task".into(),
            ])
            .centered(),
            Line::from(vec![
                "Press ".into(),
                "UP".bold(),
                " or ".into(),
                "DOWN".bold(),
                " to select a task, Press ".into(),
                "LEFT".bold(),
                " or ".into(),
                "RIGHT".bold(),
                " to adjust claim duration".into(),
            ])
            .centered(),
            Line::from(vec!["Press ".into(), "q".bold(), " to quit".into()]).centered(),
        ];
        let style = Style::default();
        let text = Text::from(lines).patch_style(style);
        let help_message = Paragraph::new(text).block(Block::default().borders(Borders::ALL));

        let inner_layout = Layout::new(
            Direction::Vertical,
            [Constraint::Percentage(100), Constraint::Length(5)],
        )
        .split(main_layout[1]);
        frame.render_stateful_widget(table, inner_layout[0], &mut self.table_state);
        frame.render_widget(help_message, inner_layout[1]);
    }
}

struct Claimer {
    inner: Arc<ClaimerInner>,
    handle: tokio::task::JoinHandle<()>,
}

impl Claimer {
    fn new(pool: Arc<Pool<TcpStream>>) -> Self {
        let inner = Arc::new(ClaimerInner::new());
        let handle = tokio::task::spawn({
            let inner = inner.clone();
            async move {
                inner.run(pool).await;
            }
        });

        Self { inner, handle }
    }
}

impl Drop for Claimer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

struct ClaimerInner {
    claim_duration: Mutex<Duration>,

    claimed: AtomicBool,
    count_ok: AtomicUsize,
    count_err_server: AtomicUsize,
    count_err_claim: AtomicUsize,
}

impl ClaimerInner {
    fn new() -> Self {
        Self {
            claim_duration: Mutex::new(Duration::from_secs(1)),
            claimed: AtomicBool::new(false),
            count_ok: AtomicUsize::new(0),
            count_err_server: AtomicUsize::new(0),
            count_err_claim: AtomicUsize::new(0),
        }
    }

    async fn run(self: Arc<Self>, pool: Arc<Pool<TcpStream>>) {
        // In a loop:
        //
        // - Grab a connection from the pool
        // - Try to use it
        loop {
            self.claimed.store(false, Ordering::SeqCst);
            sleep(Duration::from_millis(1000)).await;

            match pool.claim().await {
                Ok(mut stream) => {
                    self.claimed.store(true, Ordering::SeqCst);
                    if let Err(_err) = stream.write_all(b"hello").await {
                        self.count_err_server.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }

                    // Keep the connection alive for an extra duration
                    let duration = *self.claim_duration.lock().unwrap();
                    sleep(duration).await;

                    let mut buf = [0; 5];
                    if let Err(_err) = stream.read_exact(&mut buf[..]).await {
                        self.count_err_server.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                    assert_eq!(&buf, b"hello");
                    self.count_ok.fetch_add(1, Ordering::SeqCst);
                }
                Err(_err) => {
                    self.count_err_claim.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }
}

#[cfg(feature = "qtop")]
async fn export_stats_for_qtop(stats: qorb::pool::Stats) {
    // Build a description of the API.
    let mut api = dropshot::ApiDescription::new();
    api.register(qorb::qtop::serve_stats).unwrap();

    let log = dropshot::ConfigLogging::File {
        level: dropshot::ConfigLoggingLevel::Info,
        path: log_path(),
        if_exists: dropshot::ConfigLoggingIfExists::Truncate,
    };
    // Set up the server.
    let _server = dropshot::HttpServerStarter::new(
        &dropshot::ConfigDropshot {
            bind_address: "127.0.0.1:42069".parse().unwrap(),
            ..Default::default()
        },
        api,
        stats,
        &log.to_logger("qtop").unwrap(),
    )
    .map_err(|error| format!("failed to create server: {}", error))
    .unwrap()
    .start()
    .await
    .unwrap();
}
