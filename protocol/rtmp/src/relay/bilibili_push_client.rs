use {
    super::errors::ClientError,
    crate::{
        session::client_session::{ClientSession, ClientType},
        utils::RtmpUrlParser,
    },
    streamhub::{
        define::{BroadcastEvent, BroadcastEventReceiver, StreamHubEventSender},
        stream::StreamIdentifier,
    },
    tokio::net::TcpStream,
};

pub struct BiliBiliPushClient {
    url: String,
    client_event_consumer: BroadcastEventReceiver,
    channel_event_producer: StreamHubEventSender,
}

impl BiliBiliPushClient {
    pub fn new(
        url: String,
        consumer: BroadcastEventReceiver,
        producer: StreamHubEventSender,
    ) -> Self {
        Self {
            url,

            client_event_consumer: consumer,
            channel_event_producer: producer,
        }
    }

    pub async fn run(&mut self) -> Result<(), ClientError> {
        log::info!("bilibili push client run...");

        let mut rtmp = RtmpUrlParser::new(self.url.clone());
        rtmp.parse_url().unwrap();
        rtmp.append_port("1935".to_string());

        let address = format!("{}", rtmp.host_with_port);

        loop {
            let val = self.client_event_consumer.recv().await?;

            match val {
                BroadcastEvent::Publish { identifier } => {
                    if let StreamIdentifier::Rtmp {
                        app_name,
                        stream_name,
                    } = identifier
                    {
                        log::info!(
                            "bilibili publish app_name: {} stream_name: {} address: {}",
                            app_name.clone(),
                            stream_name.clone(),
                            address.clone()
                        );
                        let stream = TcpStream::connect(address.clone()).await?;

                        let mut client_session = ClientSession::new(
                            stream,
                            ClientType::Publish,
                            address.clone(),
                            app_name,
                            stream_name,
                            self.channel_event_producer.clone(),
                            0,
                        );

                        tokio::spawn(async move {
                            if let Err(err) = client_session.run().await {
                                log::error!(
                                    "bilibili_client_session as push client run error: {}",
                                    err
                                );
                            }
                        });
                    }
                }

                _ => {
                    log::info!("bilibili push client receive other events");
                }
            }
        }
    }
}
