use common::v1alpha8::{
    grid::EnergyMarketCodeType,
    microgrid::{MicrogridStatus, electrical_components::ElectricalComponentStateCode},
};

use crate::proto::common::v1alpha8::microgrid::electrical_components::{
    BatteryType, ElectricalComponentCategory, EvChargerType, InverterType,
};

#[allow(clippy::doc_lazy_continuation, dead_code)]
pub mod common {
    pub mod v1alpha8 {
        pub mod grid {
            #![allow(clippy::derive_partial_eq_without_eq)]
            tonic::include_proto!("frequenz.api.common.v1alpha8.grid");
        }

        pub mod microgrid {
            #![allow(clippy::derive_partial_eq_without_eq)]
            tonic::include_proto!("frequenz.api.common.v1alpha8.microgrid");
            pub mod electrical_components {
                #![allow(clippy::derive_partial_eq_without_eq)]
                tonic::include_proto!(
                    "frequenz.api.common.v1alpha8.microgrid.electrical_components"
                );
            }
            pub mod sensors {
                #![allow(clippy::derive_partial_eq_without_eq)]
                tonic::include_proto!("frequenz.api.common.v1alpha8.microgrid.sensors");
            }
        }

        pub mod metrics {
            #![allow(clippy::derive_partial_eq_without_eq)]
            tonic::include_proto!("frequenz.api.common.v1alpha8.metrics");
        }

        pub mod types {
            #![allow(clippy::derive_partial_eq_without_eq)]
            tonic::include_proto!("frequenz.api.common.v1alpha8.types");
        }
    }
}

#[allow(clippy::doc_lazy_continuation)]
pub mod microgrid {
    pub mod v1alpha18 {
        #![allow(clippy::derive_partial_eq_without_eq)]
        tonic::include_proto!("frequenz.api.microgrid.v1alpha18");
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
    (
        ElectricalComponentCategory,
        "ELECTRICAL_COMPONENT_CATEGORY_"
    ),
    (BatteryType, "BATTERY_TYPE_"),
    (InverterType, "INVERTER_TYPE_"),
    (EvChargerType, "EV_CHARGER_TYPE_"),
    (
        ElectricalComponentStateCode,
        "ELECTRICAL_COMPONENT_STATE_CODE_"
    ),
    (EnergyMarketCodeType, "ENERGY_MARKET_CODE_TYPE_"),
    (MicrogridStatus, "MICROGRID_STATUS_"),
);
