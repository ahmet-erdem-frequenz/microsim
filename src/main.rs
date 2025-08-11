mod lisp;
mod proto;
mod server;
mod timeout_tracker;

use proto::microgrid::v1alpha18::microgrid_server;
use tonic::transport::Server;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    simplelog::SimpleLogger::init(simplelog::LevelFilter::Debug, simplelog::Config::default())
        .unwrap();

    let config = lisp::Config::new("config.lisp");
    tokio::spawn(config.clone().start());
    let socket_addr = config.socket_addr();
    log::info!("Server listening on {}", socket_addr);

    let server = server::MicrogridServer::new(config);
    Server::builder()
        .add_service(microgrid_server::MicrogridServer::new(server))
        .serve(socket_addr.parse().unwrap())
        .await
        .unwrap();
}
