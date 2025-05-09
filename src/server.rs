use std::pin::Pin;
use std::time::{Duration, SystemTime};

use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;

use crate::lisp::Config;

use crate::proto::microgrid::v1::{
    microgrid_server, AckComponentErrorRequest, AddComponentBoundsRequest,
    AddComponentBoundsResponse, GetMicrogridMetadataResponse, ListComponentsRequest,
    ListComponentsResponse, ListConnectionsRequest, ListConnectionsResponse, ListSensorRequest,
    ListSensorsResponse, PutComponentInStandbyRequest, ReceiveComponentDataStreamRequest,
    ReceiveComponentDataStreamResponse, ReceiveSensorDataStreamRequest,
    ReceiveSensorDataStreamResponse, SetComponentPowerActiveRequest,
    SetComponentPowerActiveResponse, SetComponentPowerReactiveRequest,
    SetComponentPowerReactiveResponse, StartComponentRequest, StopComponentRequest,
};

pub struct MicrogridServer {
    pub config: Config,
    pub timeout_tracker: crate::timeout_tracker::TimeoutTracker,
}

impl MicrogridServer {
    pub fn new(config: Config) -> Self {
        let timeout_tracker = crate::timeout_tracker::TimeoutTracker::new();

        let new = Self {
            config,
            timeout_tracker,
        };

        new.start_timeout_tracker();
        new
    }

    fn start_timeout_tracker(&self) {
        let timeout_tracker = self.timeout_tracker.clone();
        let config = self.config.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let expired_ids = timeout_tracker.remove_expired();
                for id in expired_ids {
                    log::info!("Request timeout for component {}.", id);
                    config.reset_power_active(id).unwrap();
                }
            }
        });
    }
}

#[tonic::async_trait]
impl microgrid_server::Microgrid for MicrogridServer {
    type ReceiveComponentDataStreamStream = Pin<
        Box<dyn Stream<Item = Result<ReceiveComponentDataStreamResponse, tonic::Status>> + Send>,
    >;
    type ReceiveSensorDataStreamStream =
        Pin<Box<dyn Stream<Item = Result<ReceiveSensorDataStreamResponse, tonic::Status>> + Send>>;

    async fn get_microgrid_metadata(
        &self,
        _request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<GetMicrogridMetadataResponse>, tonic::Status> {
        let metadata = self.config.metadata().unwrap();
        Ok(tonic::Response::new(metadata))
    }

    async fn list_components(
        &self,
        _request: tonic::Request<ListComponentsRequest>,
    ) -> std::result::Result<tonic::Response<ListComponentsResponse>, tonic::Status> {
        let request = _request.into_inner();
        let components = self.config.components(request).unwrap();
        Ok(tonic::Response::new(components))
    }

    async fn list_connections(
        &self,
        _request: tonic::Request<ListConnectionsRequest>,
    ) -> std::result::Result<tonic::Response<ListConnectionsResponse>, tonic::Status> {
        let request = _request.into_inner();
        let connections = self.config.connections(request).unwrap();
        Ok(tonic::Response::new(connections))
    }

    async fn set_component_power_active(
        &self,
        _request: tonic::Request<SetComponentPowerActiveRequest>,
    ) -> std::result::Result<tonic::Response<SetComponentPowerActiveResponse>, tonic::Status> {
        let request = _request.into_inner();
        let duration = if let Some(dur) = request.request_lifetime {
            if dur < 10 || dur > 60 * 15 {
                return Err(tonic::Status::invalid_argument(
                    "Request lifetime must be between 10 seconds and 15 minutes.",
                ));
            }
            Duration::from_secs(dur)
        } else {
            self.config.retain_requests_duration()
        };
        self.timeout_tracker.add(request.component_id, duration);

        let res = self
            .config
            .set_power_active(request.component_id, request.power);

        if let Err(err) = res {
            log::error!("Tulisp error:\n{}", err.format(&self.config.ctx.borrow()));
            return Err(tonic::Status::failed_precondition(err.desc()));
        }
        Ok(tonic::Response::new(SetComponentPowerActiveResponse {
            valid_until: Some(SystemTime::now().checked_add(duration).unwrap().into()),
        }))
    }

    async fn receive_component_data_stream(
        &self,
        request: tonic::Request<ReceiveComponentDataStreamRequest>,
    ) -> std::result::Result<tonic::Response<Self::ReceiveComponentDataStreamStream>, tonic::Status>
    {
        let component_id = request.into_inner().component_id;

        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut last_msg_ts = SystemTime::now();
            loop {
                let (data, interval) = config
                    .get_component_data(component_id as u64)
                    .map_err(|e| {
                        log::error!("Tulisp error:\n{}", e.format(&config.ctx.borrow()));
                        e
                    })
                    .unwrap();

                if let Err(err) = tx.send(Result::<_, tonic::Status>::Ok(data)).await {
                    log::debug!("stream_component_data(component_id={component_id}): {err}");
                    break;
                }

                let now = SystemTime::now();
                let tgt_ts = last_msg_ts + Duration::from_millis(interval as u64);
                let dur =
                    Duration::from_millis(tgt_ts.duration_since(now).unwrap().as_millis() as u64);
                tokio::time::sleep(dur).await;
                last_msg_ts = tgt_ts;
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(
            Box::pin(output_stream) as Self::ReceiveComponentDataStreamStream
        ))
    }

    //
    //
    // Unused methods
    //
    //
    async fn list_sensors(
        &self,
        _request: tonic::Request<ListSensorRequest>,
    ) -> std::result::Result<tonic::Response<ListSensorsResponse>, tonic::Status> {
        todo!()
    }
    async fn receive_sensor_data_stream(
        &self,
        _request: tonic::Request<ReceiveSensorDataStreamRequest>,
    ) -> std::result::Result<tonic::Response<Self::ReceiveSensorDataStreamStream>, tonic::Status>
    {
        todo!()
    }
    async fn add_component_bounds(
        &self,
        _request: tonic::Request<AddComponentBoundsRequest>,
    ) -> std::result::Result<tonic::Response<AddComponentBoundsResponse>, tonic::Status> {
        todo!()
    }
    async fn set_component_power_reactive(
        &self,
        _request: tonic::Request<SetComponentPowerReactiveRequest>,
    ) -> std::result::Result<tonic::Response<SetComponentPowerReactiveResponse>, tonic::Status>
    {
        todo!()
    }
    async fn start_component(
        &self,
        _request: tonic::Request<StartComponentRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        todo!()
    }
    async fn put_component_in_standby(
        &self,
        _request: tonic::Request<PutComponentInStandbyRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        todo!()
    }
    async fn stop_component(
        &self,
        _request: tonic::Request<StopComponentRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        todo!()
    }
    async fn ack_component_error(
        &self,
        _request: tonic::Request<AckComponentErrorRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        todo!()
    }
}
