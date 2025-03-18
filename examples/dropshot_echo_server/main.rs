use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpServerStarter;
use dropshot::RequestContext;
use dropshot::TypedBody;
use std::env;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let mut schema_path = None;
    let mut iter = args[1..].iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" => {
                eprintln!(
                    "Usage: {} <options>\n\
                     Options may include:\n\
                        --help         : Show help\n\
                        --schema <PATH>: Write schema to path instead of running server\n
                    ",
                    args[0],
                );
                return;
            }
            "--schema" => {
                let Some(path) = iter.next() else {
                    eprintln!("Missing path?");
                    return;
                };
                schema_path = Some(path);
            }
            _unknown => {
                eprintln!("Unknown arg, try --help");
                return;
            }
        }
    }

    let config_dropshot: ConfigDropshot = Default::default();

    // For simplicity, we'll configure an "info"-level logger that writes to
    // stderr assuming that it's a terminal.
    let config_logging = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    };
    let log = config_logging
        .to_logger("echo-server")
        .map_err(|error| format!("failed to create logger: {}", error))
        .unwrap();

    // Build a description of the API.
    let mut api = ApiDescription::new();
    api.register(echo).unwrap();

    if let Some(schema_path) = schema_path {
        let mut file = std::fs::File::create(schema_path).unwrap();
        api.openapi("echo", "0.0.0".parse().unwrap())
            .write(&mut file)
            .unwrap();
        return;
    }

    // Set up the server.
    let server = HttpServerStarter::new(&config_dropshot, api, (), &log)
        .map_err(|error| format!("failed to create server: {}", error))
        .unwrap()
        .start();

    // Wait for the server to stop.  Note that there's not any code to shut down
    // this server, so we should never get past this point.
    server.await.unwrap();
}

#[endpoint {
    method = GET,
    path = "/echo",
}]
async fn echo(
    _rqctx: RequestContext<()>,
    body: TypedBody<String>,
) -> Result<HttpResponseOk<String>, HttpError> {
    Ok(HttpResponseOk(body.into_inner()))
}
