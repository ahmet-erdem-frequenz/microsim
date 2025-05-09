use rand::Rng;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    path::Path,
    rc::Rc,
    str::FromStr,
    time::Duration,
};

use crate::proto::{
    common::v1::{
        grid::{DeliveryArea, EnergyMarketCodeType},
        metrics::{
            metric_value_variant, Bounds, Metric, MetricSample, MetricValueVariant,
            SimpleMetricValue,
        },
        microgrid::{
            components::{
                component_category_metadata_variant::Metadata, Battery, BatteryType, Component,
                ComponentCategory, ComponentCategoryMetadataVariant, ComponentConnection,
                ComponentData, ComponentState, ComponentStateCode, EvCharger, EvChargerType,
                GridConnectionPoint, Inverter, InverterType,
            },
            MicrogridStatus,
        },
    },
    microgrid::v1::{
        GetMicrogridMetadataResponse, ListComponentsRequest, ListComponentsResponse,
        ListConnectionsRequest, ListConnectionsResponse, ReceiveComponentDataStreamResponse,
    },
};
use notify::{RecommendedWatcher, Watcher};
use prost_types::Timestamp;
use tulisp::{destruct_bind, intern, list, Error, ErrorKind, TulispContext, TulispObject};

type CompDataMaker = fn(
    &mut TulispContext,
    &TulispObject,
    &Symbols,
) -> Result<ReceiveComponentDataStreamResponse, Error>;

intern! {
    #[derive(Clone)]
    pub(crate) struct Symbols {
        id: "id",
        soc: "soc",
        name: "name",
        data: "data",
        type_: "type",
        power: "power",
        status: "status",
        stream: "stream",
        voltage: "voltage",
        current: "current",
        category: "category",
        interval: "interval",
        capacity: "capacity",
        location: "location",
        metadata: "metadata",
        soc_lower: "soc-lower",
        soc_upper: "soc-upper",
        relay_state: "relay-state",
        cable_state: "cable-state",
        socket_addr: "socket-addr",
        ac_frequency: "ac-frequency",
        microgrid_id: "microgrid-id",
        enterprise_id: "enterprise-id",
        delivery_area: "delivery-area",
        inclusion_lower: "inclusion-lower",
        inclusion_upper: "inclusion-upper",
        exclusion_lower: "exclusion-lower",
        exclusion_upper: "exclusion-upper",
        per_phase_power: "per-phase-power",
        component_state: "component-state",
        components_alist: "components-alist",
        set_power_active: "set-power-active",
        create_timestamp: "create-timestamp",
        connections_alist: "connections-alist",
        rated_fuse_current: "rated-fuse-current",
        reset_power_active: "reset-power-active",
        state_update_functions: "state-update-functions",
        state_update_interval_ms: "state-update-interval-ms",
        retain_requests_duration_ms: "retain-requests-duration-ms",
    }
}

#[derive(Clone)]
pub struct Config {
    filename: String,

    pub(crate) ctx: Rc<RefCell<tulisp::TulispContext>>,

    /// Component ID -> (Component's Data Method, Interval, To ComponentData Method)
    stream_methods: Rc<RefCell<HashMap<u64, (TulispObject, u64, CompDataMaker)>>>,

    /// Component ID -> last power update time.
    last_formula_update_time: Rc<RefCell<std::time::Instant>>,

    default_request_duration: Cell<Option<Duration>>,

    symbols: Symbols,
}

// Tokio is configured to use the current_thread runtime, so it is not unsafe to
// make `Config` Send and Sync.
unsafe impl Send for Config {}
unsafe impl Sync for Config {}

macro_rules! alist_get_as {
    ($ctx: expr, $rest:expr, $key:expr, $as_fn:ident) => {{
        alist_get_as!($ctx, $rest, $key).and_then(|x| x.$as_fn())
    }};
    ($ctx: expr, $rest:expr, $key:expr, eval++$as_fn:ident) => {{
        let out = alist_get_as!($ctx, $rest, $key);
        out.and_then(|x| $ctx.eval_and_then(&x, |x| x.$as_fn()))
    }};
    ($ctx: expr, $rest:expr, $key:expr) => {{
        tulisp::lists::alist_get($ctx, $key, $rest, None, None, None)
    }};
}

macro_rules! alist_get_f32 {
    ($ctx: expr, $rest:expr, $key:expr) => {
        alist_get_as!($ctx, $rest, $key, eval ++ try_float).unwrap_or_default() as f32
    };
}

macro_rules! alist_get_u32 {
    ($ctx: expr, $rest:expr, $key:expr) => {
        alist_get_as!($ctx, $rest, $key, eval ++ try_int).unwrap_or_default() as u32
    };
}

macro_rules! alist_get_3_phase {
    ($ctx: expr, $rest:expr, $key:expr) => {{
        let expr = alist_get_as!($ctx, $rest, $key).unwrap_or_default();
        let items = if expr.consp() && expr.car_and_then(|x| Ok(x.numberp()))? {
            expr
        } else {
            $ctx.eval(&expr)?
        };
        (
            items
                .car()
                .and_then(|x| $ctx.eval_and_then(&x, |x| x.as_float()))
                .unwrap_or_default() as f32,
            items
                .cadr()
                .and_then(|x| $ctx.eval_and_then(&x, |x| x.as_float()))
                .unwrap_or_default() as f32,
            items
                .caddr()
                .and_then(|x| $ctx.eval_and_then(&x, |x| x.as_float()))
                .unwrap_or_default() as f32,
        )
    }};
}

fn enum_from_alist<T: FromStr + Default>(
    ctx: &mut TulispContext,
    alist: &TulispObject,
    key: &TulispObject,
    eval: bool,
) -> Option<T> {
    let val = if eval {
        alist_get_as!(ctx, alist, key, eval ++ as_symbol).ok()?
    } else {
        alist_get_as!(ctx, alist, key, as_symbol).ok()?
    };
    match val.parse::<T>() {
        Ok(x) => Some(x),
        Err(_) => {
            log::error!("Invalid value for {}: {}", key, val);
            None
        }
    }
}

fn make_component_from_alist(
    ctx: &mut TulispContext,
    alist: &TulispObject,
    symbols: &Symbols,
) -> Result<Component, Error> {
    let id = alist_get_as!(ctx, alist, &symbols.id, as_int)? as u64;
    let name = alist_get_as!(ctx, alist, &symbols.name, as_string).unwrap_or_default();
    let Some(category) = enum_from_alist::<ComponentCategory>(ctx, alist, &symbols.category, false)
    else {
        return Err(Error::new(
            tulisp::ErrorKind::Uninitialized,
            format!("Invalid component category for component {}", id),
        ));
    };

    let metadata = match category {
        ComponentCategory::Inverter => Some(Metadata::Inverter(Inverter {
            r#type: enum_from_alist::<InverterType>(ctx, alist, &symbols.type_, false)
                .map(|typ| typ as i32)
                .unwrap_or_default(),
        })),
        ComponentCategory::Battery => Some(Metadata::Battery(Battery {
            r#type: enum_from_alist::<BatteryType>(ctx, alist, &symbols.type_, false)
                .map(|typ| typ as i32)
                .unwrap_or_default(),
        })),
        ComponentCategory::EvCharger => Some(Metadata::EvCharger(EvCharger {
            r#type: enum_from_alist::<EvChargerType>(ctx, alist, &symbols.type_, false)
                .map(|typ| typ as i32)
                .unwrap_or_default(),
        })),
        ComponentCategory::Grid => Some(Metadata::Grid(GridConnectionPoint {
            rated_fuse_current: alist_get_u32!(ctx, alist, &symbols.rated_fuse_current),
        })),
        _ => None,
    };

    let comp = Component {
        id,
        name,
        category: category as i32,
        microgrid_id: 0, // TODO: Add microgrid_id
        category_type: Some(ComponentCategoryMetadataVariant { metadata }),
        // status: todo!(),  // TODO: Add status
        // operational_lifetime: todo!(),
        // metric_config_bounds: todo!(),   // TODO: Add bounds
        ..Default::default()
    };

    Ok(comp)
}

impl Config {
    pub fn new(filename: &str) -> Self {
        let mut ctx = tulisp::TulispContext::new();
        add_functions(&mut ctx);

        let _ = ctx.eval_file(filename).map_err(|e| {
            log::error!("Tulisp error:\n{}", e.format(&ctx));
            e
        });
        let now = std::time::Instant::now();
        let symbols = Symbols::new(&mut ctx);
        Self {
            filename: filename.to_string(),
            ctx: Rc::new(RefCell::new(ctx)),
            stream_methods: Rc::new(RefCell::new(HashMap::new())),
            last_formula_update_time: Rc::new(RefCell::new(now)),
            default_request_duration: Cell::new(None),
            symbols,
        }
    }

    pub fn reload(&self) {
        let start = std::time::Instant::now();
        let mut ctx = self.ctx.borrow_mut();
        if ctx
            .eval_file(&self.filename)
            .map_err(|e| {
                log::error!("Tulisp error:\n{}", e.format(&ctx));
                e
            })
            .is_err()
        {
            return;
        }
        let duration = start.elapsed();
        log::info!(
            "Reloaded config file in {}ms",
            duration.as_nanos() as f64 / 1e6
        );
        *self.stream_methods.borrow_mut() = HashMap::new();
    }

    pub async fn start(self) {
        self.start_state_updates();
        self.start_watching().await;
    }

    async fn start_watching(self) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        let mut watcher = RecommendedWatcher::new(
            move |res| {
                futures::executor::block_on(async {
                    tx.send(res).await.unwrap();
                });
            },
            notify::Config::default(),
        )
        .unwrap();
        watcher
            .watch(
                &Path::new(&self.filename),
                notify::RecursiveMode::NonRecursive,
            )
            .unwrap();

        while let Some(res) = rx.recv().await {
            match res {
                Ok(event) => {
                    if let notify::EventKind::Modify(_) = event.kind {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        self.reload();
                    }
                }
                Err(e) => {
                    log::error!("watch error: {:?}", e);
                    return;
                }
            }
        }
    }

    fn start_state_updates(&self) {
        let config = self.clone();
        tokio::spawn(async move {
            loop {
                config.update_state();
                let update_interval = config
                    .symbols
                    .state_update_interval_ms
                    .get()
                    .and_then(|x| x.as_int())
                    .unwrap_or(2000) as u64;
                tokio::time::sleep(Duration::from_millis(update_interval)).await;
            }
        });
    }

    fn update_state(&self) {
        let exprs_alist = self
            .symbols
            .state_update_functions
            .get()
            .map_err(|e| {
                log::error!("Tulisp error:\n{}", e.format(&self.ctx.borrow()));
                panic!("Update state function failed");
            })
            .unwrap();
        let last_update_time = self.last_formula_update_time.borrow();
        let now = std::time::Instant::now();

        for func in exprs_alist.base_iter() {
            let res = self.ctx.borrow_mut().funcall(
                &func,
                &list![(now.duration_since(*last_update_time).as_millis() as i64).into()].unwrap(),
            );
            res.map_err(|e| {
                log::error!("Tulisp error:\n{}", e.format(&self.ctx.borrow()));
                panic!("Update state function failed");
            })
            .unwrap();
        }
        drop(last_update_time);
        *self.last_formula_update_time.borrow_mut() = now;
    }

    pub fn socket_addr(&self) -> String {
        let addr = self.symbols.socket_addr.get().and_then(|x| x.as_string());

        match addr {
            Ok(vv) => vv,
            Err(err) => {
                panic!(
                    r#"{}

Invalid socket-addr.  Add a config line in this format:
	(setq socket-addr "[::1]:8080")
"#,
                    err.format(&self.ctx.borrow())
                )
            }
        }
    }

    pub fn retain_requests_duration(&self) -> Duration {
        if let Some(dur) = self.default_request_duration.get() {
            return dur;
        }
        let dur_ms = self
            .symbols
            .retain_requests_duration_ms
            .get()
            .and_then(|x| x.as_int())
            .unwrap_or(5000);

        let dur = Duration::from_millis(dur_ms as u64);
        self.default_request_duration.set(Some(dur));
        dur
    }

    pub fn metadata(&self) -> Result<GetMicrogridMetadataResponse, Error> {
        let alist = self
            .symbols
            .metadata
            .get()
            .unwrap_or_else(|_| TulispObject::nil());

        let microgrid_id = alist_get_as!(
            &mut self.ctx.borrow_mut(),
            &alist,
            &self.symbols.microgrid_id,
            as_int
        )
        .unwrap_or_default() as u64;

        let enterprise_id = alist_get_as!(
            &mut self.ctx.borrow_mut(),
            &alist,
            &self.symbols.enterprise_id,
            as_int
        )
        .unwrap_or_default() as u64;

        let delivery_area = if let Ok(delivery_area) = alist_get_as!(
            &mut self.ctx.borrow_mut(),
            &alist,
            &self.symbols.delivery_area
        ) {
            Some(DeliveryArea {
                code: delivery_area.car()?.as_string().unwrap_or_default(),
                code_type: delivery_area
                    .cadr()?
                    .as_symbol()?
                    .parse::<EnergyMarketCodeType>()
                    .unwrap_or_default() as i32,
            })
        } else {
            None
        };

        let location = if let Ok(location) =
            alist_get_as!(&mut self.ctx.borrow_mut(), &alist, &self.symbols.location)
        {
            Some(crate::proto::common::v1::Location {
                latitude: location.car()?.as_float().unwrap_or_default() as f32,
                longitude: location.cadr()?.as_float().unwrap_or_default() as f32,
                country_code: location.caddr()?.as_string().unwrap_or_default(),
            })
        } else {
            None
        };

        let status = alist_get_as!(
            &mut self.ctx.borrow_mut(),
            &alist,
            &self.symbols.status,
            as_symbol
        )
        .unwrap_or_default()
        .parse::<MicrogridStatus>()
        .unwrap_or_default() as i32;

        let create_timestamp = if let Ok(iso_ts) = alist_get_as!(
            &mut self.ctx.borrow_mut(),
            &alist,
            &self.symbols.create_timestamp,
            as_string
        ) {
            Some(Timestamp::from_str(iso_ts.as_str()).unwrap_or_default())
        } else {
            None
        };

        Ok(GetMicrogridMetadataResponse {
            microgrid: Some(crate::proto::common::v1::microgrid::Microgrid {
                id: microgrid_id,
                enterprise_id,
                name: format!("Microgrid {}", microgrid_id),
                delivery_area,
                location,
                status,
                create_timestamp,
            }),
        })
    }

    pub fn components(
        &self,
        request: ListComponentsRequest,
    ) -> Result<ListComponentsResponse, Error> {
        let alists = self.symbols.components_alist.get()?;

        Ok(ListComponentsResponse {
            components: alists
                .base_iter()
                .map(|x| {
                    make_component_from_alist(&mut self.ctx.borrow_mut(), &x, &self.symbols)
                        .unwrap()
                })
                .filter(|x| {
                    (request.component_ids.contains(&x.id) || request.component_ids.is_empty())
                        && (request.categories.contains(&x.category)
                            || request.categories.is_empty())
                })
                .collect(),
        })
    }

    pub fn connections(
        &self,
        request: ListConnectionsRequest,
    ) -> Result<ListConnectionsResponse, Error> {
        let alist = self.symbols.connections_alist.get()?;
        Ok(ListConnectionsResponse {
            connections: alist
                .base_iter()
                .map(|x| ComponentConnection {
                    source_component_id: x.car().and_then(|x| x.as_int()).unwrap() as u64,
                    destination_component_id: x.cdr().and_then(|x| x.as_int()).unwrap() as u64,
                    ..Default::default()
                })
                .filter(|x| {
                    (request.starts.contains(&x.source_component_id) || request.starts.is_empty())
                        && (request.ends.contains(&x.destination_component_id)
                            || request.ends.is_empty())
                })
                .collect(),
        })
    }

    pub fn set_power_active(&self, component_id: u64, power: f32) -> Result<(), Error> {
        let res = self.ctx.borrow_mut().funcall(
            &self.symbols.set_power_active,
            &list![(component_id as i64).into(), (power as f64).into()]?,
        )?;

        if !res.null() {
            return Err(Error::new(tulisp::ErrorKind::Undefined, res.as_string()?).with_trace(res));
        }
        Ok(())
    }

    pub fn reset_power_active(&self, component_id: u64) -> Result<(), Error> {
        fn work(config: &Config, component_id: u64) -> Result<(), Error> {
            config.ctx.borrow_mut().funcall(
                &config.symbols.reset_power_active,
                &list![(component_id as i64).into()]?,
            )?;

            Ok(())
        }
        work(self, component_id)
            .inspect_err(|e| log::error!("Tulisp error:\n{}", e.format(&self.ctx.borrow())))
    }

    fn get_conv_function(&self, component_id: u64, comp: &TulispObject) -> CompDataMaker {
        match make_component_from_alist(&mut self.ctx.borrow_mut(), &comp, &self.symbols)
            .unwrap()
            .category()
        {
            ComponentCategory::Battery => Self::battery_data,
            ComponentCategory::Inverter => Self::inverter_data,
            ComponentCategory::Meter => Self::meter_data,
            ComponentCategory::EvCharger => Self::ev_charger_data,
            _ => Err(Error::new(
                tulisp::ErrorKind::Uninitialized,
                format!("Invalid component category for component {}", component_id),
            ))
            .unwrap(),
        }
    }

    pub fn get_component_data(
        &self,
        component_id: u64,
    ) -> Result<(ReceiveComponentDataStreamResponse, u64), Error> {
        let mut stream_methods = self.stream_methods.borrow_mut();
        let (data_method, interval, conv_function) =
            if let Some((data_method, interval, conv_function)) = stream_methods.get(&component_id)
            {
                (data_method.clone(), *interval, *conv_function)
            } else {
                let alists = self.symbols.components_alist.get()?;
                let comp = alists
                    .base_iter()
                    .find(|x| {
                        alist_get_as!(&mut self.ctx.borrow_mut(), &x, &self.symbols.id, as_int)
                            .unwrap() as u64
                            == component_id
                    })
                    .expect(&format!("Component id {component_id} not found"));

                let stream =
                    alist_get_as!(&mut self.ctx.borrow_mut(), &comp, &self.symbols.stream).unwrap();

                let interval = alist_get_as!(
                    &mut self.ctx.borrow_mut(),
                    &stream,
                    &self.symbols.interval,
                    as_int
                )
                .unwrap();
                let data_method =
                    alist_get_as!(&mut self.ctx.borrow_mut(), &stream, &self.symbols.data).unwrap();

                let conv_function = self.get_conv_function(component_id, &comp);

                stream_methods.insert(
                    component_id,
                    (data_method.clone(), interval as u64, conv_function),
                );

                (data_method, interval as u64, conv_function)
            };

        let tulisp_data = self
            .ctx
            .borrow_mut()
            .funcall(&data_method, &list!((component_id as i64).into())?);
        let tulisp_data = tulisp_data.map_err(|e| {
            log::error!("Tulisp error:\n{}", e.format(&self.ctx.borrow()));
            panic!();
        })?;

        let comp_data = conv_function(&mut self.ctx.borrow_mut(), &tulisp_data, &self.symbols);
        let comp_data = comp_data.map_err(|e| {
            log::error!("Tulisp error:\n{}", e.format(&self.ctx.borrow()));
            panic!();
        })?;

        Ok((comp_data, interval as u64))
    }
}

/// ComponentData methods
impl Config {
    fn battery_data(
        ctx: &mut TulispContext,
        alist: &TulispObject,
        symbols: &Symbols,
    ) -> Result<ReceiveComponentDataStreamResponse, Error> {
        let id = alist_get_as!(ctx, &alist, &symbols.id, eval ++ as_int)? as u64;
        let capacity = alist_get_f32!(ctx, &alist, &symbols.capacity);

        let soc = alist_get_f32!(ctx, &alist, &symbols.soc);
        let soc_lower = alist_get_f32!(ctx, &alist, &symbols.soc_lower);
        let soc_upper = alist_get_f32!(ctx, &alist, &symbols.soc_upper);

        let voltage = alist_get_f32!(ctx, &alist, &symbols.voltage);
        let current = alist_get_f32!(ctx, &alist, &symbols.current);
        let power = alist_get_f32!(ctx, &alist, &symbols.power);

        let inclusion_lower = alist_get_f32!(ctx, &alist, &symbols.inclusion_lower);
        let inclusion_upper = alist_get_f32!(ctx, &alist, &symbols.inclusion_upper);
        let exclusion_lower = alist_get_f32!(ctx, &alist, &symbols.exclusion_lower);
        let exclusion_upper = alist_get_f32!(ctx, &alist, &symbols.exclusion_upper);

        let component_state =
            enum_from_alist::<ComponentStateCode>(ctx, &alist, &symbols.component_state, true)
                .unwrap_or_default() as i32;
        let relay_state =
            enum_from_alist::<ComponentStateCode>(ctx, &alist, &symbols.relay_state, true)
                .unwrap_or_default() as i32;

        let now = Some(Timestamp::from(std::time::SystemTime::now()));

        Ok(ReceiveComponentDataStreamResponse {
            data: Some(ComponentData {
                component_id: id,
                metric_samples: vec![
                    MetricSample {
                        sampled_at: now,
                        metric: Metric::BatteryCapacity as i32,
                        value: Some(MetricValueVariant {
                            metric_value_variant: Some(
                                metric_value_variant::MetricValueVariant::SimpleMetric(
                                    SimpleMetricValue { value: capacity },
                                ),
                            ),
                        }),
                        ..Default::default() // TODO: Add bounds and states
                    },
                    MetricSample {
                        sampled_at: now,
                        metric: Metric::BatterySocPct as i32,
                        value: Some(MetricValueVariant {
                            metric_value_variant: Some(
                                metric_value_variant::MetricValueVariant::SimpleMetric(
                                    SimpleMetricValue { value: soc },
                                ),
                            ),
                        }),
                        bounds: vec![Bounds {
                            lower: Some(soc_lower),
                            upper: Some(soc_upper),
                        }],
                        ..Default::default() // TODO: Add bounds and states
                    },
                    MetricSample {
                        sampled_at: now,
                        metric: Metric::AcVoltage as i32,
                        value: Some(MetricValueVariant {
                            metric_value_variant: Some(
                                metric_value_variant::MetricValueVariant::SimpleMetric(
                                    SimpleMetricValue { value: voltage },
                                ),
                            ),
                        }),
                        ..Default::default() // TODO: Add bounds and states
                    },
                    MetricSample {
                        sampled_at: now,
                        metric: Metric::AcCurrent as i32,
                        value: Some(MetricValueVariant {
                            metric_value_variant: Some(
                                metric_value_variant::MetricValueVariant::SimpleMetric(
                                    SimpleMetricValue { value: current },
                                ),
                            ),
                        }),
                        ..Default::default() // TODO: Add bounds and states
                    },
                    MetricSample {
                        sampled_at: now,
                        metric: Metric::AcActivePower as i32,
                        value: Some(MetricValueVariant {
                            metric_value_variant: Some(
                                metric_value_variant::MetricValueVariant::SimpleMetric(
                                    SimpleMetricValue { value: power },
                                ),
                            ),
                        }),
                        bounds: if exclusion_lower == 0.0 && exclusion_upper == 0.0 {
                            vec![Bounds {
                                lower: Some(inclusion_lower),
                                upper: Some(inclusion_upper),
                            }]
                        } else {
                            vec![
                                Bounds {
                                    lower: Some(inclusion_lower),
                                    upper: Some(exclusion_lower),
                                },
                                Bounds {
                                    lower: Some(exclusion_upper),
                                    upper: Some(inclusion_upper),
                                },
                            ]
                        },
                        ..Default::default() // TODO: Add bounds and states
                    },
                ],
                states: vec![ComponentState {
                    sampled_at: now,
                    states: vec![component_state, relay_state],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        })
    }

    fn ac_from_alist(
        ctx: &mut TulispContext,
        now: Option<Timestamp>,
        alist: &TulispObject,
        symbols: &Symbols,
    ) -> Result<Vec<MetricSample>, Error> {
        let frequency = symbols
            .ac_frequency
            .get()
            .and_then(|x| x.as_float())
            .unwrap_or_default() as f32;
        let current = alist_get_3_phase!(ctx, &alist, &symbols.current);
        let voltage = alist_get_3_phase!(ctx, &alist, &symbols.voltage);
        let per_phase_power = alist_get_3_phase!(ctx, &alist, &symbols.per_phase_power);

        let power = alist_get_f32!(ctx, &alist, &symbols.power);

        let inclusion_lower = alist_get_f32!(ctx, &alist, &symbols.inclusion_lower);
        let inclusion_upper = alist_get_f32!(ctx, &alist, &symbols.inclusion_upper);
        let exclusion_lower = alist_get_f32!(ctx, &alist, &symbols.exclusion_lower);
        let exclusion_upper = alist_get_f32!(ctx, &alist, &symbols.exclusion_upper);

        Ok(vec![
            MetricSample {
                sampled_at: now,
                metric: Metric::AcFrequency as i32,
                value: Some(MetricValueVariant {
                    metric_value_variant: Some(
                        metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                            value: frequency,
                        }),
                    ),
                }),
                ..Default::default()
            },
            MetricSample {
                sampled_at: now,
                metric: Metric::AcVoltagePhase1N as i32,
                value: Some(MetricValueVariant {
                    metric_value_variant: Some(
                        metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                            value: voltage.0,
                        }),
                    ),
                }),
                ..Default::default()
            },
            MetricSample {
                sampled_at: now,
                metric: Metric::AcVoltagePhase2N as i32,
                value: Some(MetricValueVariant {
                    metric_value_variant: Some(
                        metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                            value: voltage.1,
                        }),
                    ),
                }),
                ..Default::default()
            },
            MetricSample {
                sampled_at: now,
                metric: Metric::AcVoltagePhase3N as i32,
                value: Some(MetricValueVariant {
                    metric_value_variant: Some(
                        metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                            value: voltage.2,
                        }),
                    ),
                }),
                ..Default::default()
            },
            MetricSample {
                sampled_at: now,
                metric: Metric::AcCurrentPhase1 as i32,
                value: Some(MetricValueVariant {
                    metric_value_variant: Some(
                        metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                            value: current.0,
                        }),
                    ),
                }),
                ..Default::default()
            },
            MetricSample {
                sampled_at: now,
                metric: Metric::AcCurrentPhase2 as i32,
                value: Some(MetricValueVariant {
                    metric_value_variant: Some(
                        metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                            value: current.1,
                        }),
                    ),
                }),
                ..Default::default()
            },
            MetricSample {
                sampled_at: now,
                metric: Metric::AcCurrentPhase3 as i32,
                value: Some(MetricValueVariant {
                    metric_value_variant: Some(
                        metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                            value: current.2,
                        }),
                    ),
                }),
                ..Default::default()
            },
            MetricSample {
                sampled_at: now,
                metric: Metric::AcActivePowerPhase1 as i32,
                value: Some(MetricValueVariant {
                    metric_value_variant: Some(
                        metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                            value: per_phase_power.0,
                        }),
                    ),
                }),
                ..Default::default()
            },
            MetricSample {
                sampled_at: now,
                metric: Metric::AcActivePowerPhase2 as i32,
                value: Some(MetricValueVariant {
                    metric_value_variant: Some(
                        metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                            value: per_phase_power.1,
                        }),
                    ),
                }),
                ..Default::default()
            },
            MetricSample {
                sampled_at: now,
                metric: Metric::AcActivePowerPhase3 as i32,
                value: Some(MetricValueVariant {
                    metric_value_variant: Some(
                        metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                            value: per_phase_power.2,
                        }),
                    ),
                }),
                ..Default::default()
            },
            MetricSample {
                sampled_at: now,
                metric: Metric::AcActivePower as i32,
                value: Some(MetricValueVariant {
                    metric_value_variant: Some(
                        metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                            value: power,
                        }),
                    ),
                }),
                bounds: if exclusion_lower == 0.0 && exclusion_upper == 0.0 {
                    vec![Bounds {
                        lower: Some(inclusion_lower),
                        upper: Some(inclusion_upper),
                    }]
                } else {
                    vec![
                        Bounds {
                            lower: Some(inclusion_lower),
                            upper: Some(exclusion_lower),
                        },
                        Bounds {
                            lower: Some(exclusion_upper),
                            upper: Some(inclusion_upper),
                        },
                    ]
                },
                ..Default::default()
            },
        ])
    }

    fn inverter_data(
        ctx: &mut TulispContext,
        alist: &TulispObject,
        symbols: &Symbols,
    ) -> Result<ReceiveComponentDataStreamResponse, Error> {
        let id = alist_get_as!(ctx, &alist, &symbols.id, eval ++ as_int)? as u64;

        let component_state =
            enum_from_alist::<ComponentStateCode>(ctx, &alist, &symbols.component_state, true)
                .unwrap_or_default() as i32;

        let now = Some(Timestamp::from(std::time::SystemTime::now()));

        Ok(ReceiveComponentDataStreamResponse {
            data: Some(ComponentData {
                component_id: id,
                metric_samples: Self::ac_from_alist(ctx, now, alist, symbols)?,
                states: vec![ComponentState {
                    sampled_at: now,
                    states: vec![component_state],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        })
    }

    fn meter_data(
        ctx: &mut TulispContext,
        alist: &TulispObject,
        symbols: &Symbols,
    ) -> Result<ReceiveComponentDataStreamResponse, Error> {
        let id = alist_get_as!(ctx, &alist, &symbols.id, eval ++ as_int)? as u64;

        let now = Some(Timestamp::from(std::time::SystemTime::now()));

        Ok(ReceiveComponentDataStreamResponse {
            data: Some(ComponentData {
                component_id: id,
                metric_samples: Self::ac_from_alist(ctx, now, alist, symbols)?,
                states: vec![ComponentState {
                    sampled_at: now,
                    states: vec![enum_from_alist::<ComponentStateCode>(
                        ctx,
                        &alist,
                        &symbols.component_state,
                        true,
                    )
                    .unwrap_or_default() as i32],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        })
    }

    fn ev_charger_data(
        ctx: &mut TulispContext,
        alist: &TulispObject,
        symbols: &Symbols,
    ) -> Result<ReceiveComponentDataStreamResponse, Error> {
        let id = alist_get_as!(ctx, &alist, &symbols.id, eval ++ as_int)? as u64;

        let component_state =
            enum_from_alist::<ComponentStateCode>(ctx, &alist, &symbols.component_state, true)
                .unwrap_or_default() as i32;

        let cable_state =
            enum_from_alist::<ComponentStateCode>(ctx, &alist, &symbols.cable_state, true)
                .unwrap_or_default() as i32;

        let now = Some(Timestamp::from(std::time::SystemTime::now()));

        Ok(ReceiveComponentDataStreamResponse {
            data: Some(ComponentData {
                component_id: id,
                metric_samples: vec![],
                states: vec![ComponentState {
                    sampled_at: now,
                    states: vec![component_state, cable_state],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        })
    }
}

fn add_functions(ctx: &mut TulispContext) {
    macro_rules! log_impl {
        ($level:ident) => {
            |ctx, args| {
                destruct_bind!((msg) = args);
                log::$level!("{}", ctx.eval(&msg)?.as_string()?);
                Ok(TulispObject::nil())
            }
        };
    }

    ctx.add_special_form("log.info", log_impl!(info));
    ctx.add_special_form("log.warn", log_impl!(warn));
    ctx.add_special_form("log.error", log_impl!(error));
    ctx.add_special_form("log.debug", log_impl!(debug));
    ctx.add_special_form("log.trace", log_impl!(trace));

    ctx.add_special_form("random", |ctx, args| {
        destruct_bind!((&optional limit) = args);
        let rnd = if limit.null() {
            rand::thread_rng().gen()
        } else {
            let limit = ctx.eval(&limit)?.try_into()?;
            rand::thread_rng().gen_range(0..limit)
        };
        Ok(rnd.into())
    });
}
