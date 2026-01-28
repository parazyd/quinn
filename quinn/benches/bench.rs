use quinn_smol as quinn;

use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr, UdpSocket},
    sync::Arc,
    thread,
};

use bencher::{Bencher, benchmark_group, benchmark_main};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tracing::error_span;
use tracing_futures::Instrument as _;

use quinn::{Endpoint, SmolRuntime};

benchmark_group!(
    benches,
    large_data_1_stream,
    large_data_10_streams,
    small_data_1_stream,
    small_data_100_streams
);
benchmark_main!(benches);

fn large_data_1_stream(bench: &mut Bencher) {
    send_data(bench, LARGE_DATA, 1);
}

fn large_data_10_streams(bench: &mut Bencher) {
    send_data(bench, LARGE_DATA, 10);
}

fn small_data_1_stream(bench: &mut Bencher) {
    send_data(bench, SMALL_DATA, 1);
}

fn small_data_100_streams(bench: &mut Bencher) {
    send_data(bench, SMALL_DATA, 100);
}

fn send_data(bench: &mut Bencher, data: &'static [u8], concurrent_streams: usize) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = Context::new();
    let (addr, thread) = ctx.spawn_server();
    let (endpoint, client) = ctx.make_client(addr);
    let client = Arc::new(client);

    bench.bytes = (data.len() as u64) * (concurrent_streams as u64);
    bench.iter(|| {
        let mut handles = Vec::new();

        for _ in 0..concurrent_streams {
            let client = client.clone();
            handles.push(smol::spawn(async move {
                let mut stream = client.open_uni().await.unwrap();
                stream.write_all(data).await.unwrap();
                stream.finish().unwrap();
                // Wait for stream to close
                _ = stream.stopped().await;
            }));
        }

        smol::block_on(async {
            for handle in handles {
                handle.await;
            }
        });
    });
    drop(client);
    smol::block_on(endpoint.wait_idle());
    thread.join().unwrap()
}

struct Context {
    server_config: quinn::ServerConfig,
    client_config: quinn::ClientConfig,
}

impl Context {
    fn new() -> Self {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let cert = CertificateDer::from(cert.cert);

        let mut server_config =
            quinn::ServerConfig::with_single_cert(vec![cert.clone()], key.into()).unwrap();
        let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
        transport_config.max_concurrent_uni_streams(1024_u16.into());

        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).unwrap();

        Self {
            server_config,
            client_config: quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
        }
    }

    pub(crate) fn spawn_server(&self) -> (SocketAddr, thread::JoinHandle<()>) {
        let sock = UdpSocket::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0)).unwrap();
        let addr = sock.local_addr().unwrap();
        let config = self.server_config.clone();
        let handle = thread::spawn(move || {
            let endpoint = Endpoint::new(
                Default::default(),
                Some(config),
                sock,
                Arc::new(SmolRuntime),
            )
            .unwrap();

            smol::block_on(
                async move {
                    let connection = endpoint
                        .accept()
                        .await
                        .expect("accept")
                        .await
                        .expect("connect");

                    while let Ok(mut stream) = connection.accept_uni().await {
                        smol::spawn(async move {
                            while stream
                                .read_chunk(usize::MAX, false)
                                .await
                                .unwrap()
                                .is_some()
                            {}
                        })
                        .detach();
                    }
                }
                .instrument(error_span!("server")),
            );
        });
        (addr, handle)
    }

    pub(crate) fn make_client(
        &self,
        server_addr: SocketAddr,
    ) -> (quinn::Endpoint, quinn::Connection) {
        let endpoint =
            Endpoint::client(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0)).unwrap();
        let connection = smol::block_on(async {
            endpoint
                .connect_with(self.client_config.clone(), server_addr, "localhost")
                .unwrap()
                .instrument(error_span!("client"))
                .await
        })
        .unwrap();
        (endpoint, connection)
    }
}

const LARGE_DATA: &[u8] = &[0xAB; 1024 * 1024];

const SMALL_DATA: &[u8] = &[0xAB; 1];
