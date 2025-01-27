use common::v1::microgrid::components::ComponentStateCode;

use crate::proto::common::v1::microgrid::components::{
    BatteryType, ComponentCategory, EvChargerType, InverterType,
};

pub mod common {
    pub mod v1 {
        #![allow(clippy::derive_partial_eq_without_eq, clippy::doc_lazy_continuation)]
        tonic::include_proto!("frequenz.api.common.v1");

        pub mod grid {
            #![allow(clippy::derive_partial_eq_without_eq, clippy::doc_lazy_continuation)]
            tonic::include_proto!("frequenz.api.common.v1.grid");
        }

        pub mod metrics {
            #![allow(clippy::derive_partial_eq_without_eq, clippy::doc_lazy_continuation)]
            tonic::include_proto!("frequenz.api.common.v1.metrics");
        }

        pub mod microgrid {
            #![allow(clippy::derive_partial_eq_without_eq, clippy::doc_lazy_continuation)]
            tonic::include_proto!("frequenz.api.common.v1.microgrid");

            pub mod components {
                #![allow(clippy::derive_partial_eq_without_eq, clippy::doc_lazy_continuation)]
                tonic::include_proto!("frequenz.api.common.v1.microgrid.components");
            }

            pub mod sensors {
                #![allow(clippy::derive_partial_eq_without_eq, clippy::doc_lazy_continuation)]
                tonic::include_proto!("frequenz.api.common.v1.microgrid.sensors");
            }
        }
    }
}

pub mod microgrid {

    pub mod v1 {
        #![allow(clippy::derive_partial_eq_without_eq, clippy::doc_lazy_continuation)]
        tonic::include_proto!("frequenz.api.microgrid.v1");
    }
}

macro_rules! impl_enum_from_str {
    ($(($t:ty, $p:literal),)+) => {
        $(
            impl std::str::FromStr for $t {
                type Err = ();

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    let s = s.replace("-", "_");
                    match <$t>::from_str_name(($p.to_string() + &s).to_uppercase().as_str()) {
                        Some(x) => Ok(x),
                        None => Err(()),
                    }
                }
            }
        )+
    };
}

impl_enum_from_str!(
    (ComponentCategory, "COMPONENT_CATEGORY_"),
    (BatteryType, "BATTERY_TYPE_"),
    (InverterType, "INVERTER_TYPE_"),
    (EvChargerType, "EV_CHARGER_TYPE_"),
    (ComponentStateCode, "COMPONENT_STATE_CODE_"),
);
