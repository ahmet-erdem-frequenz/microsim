use std::{any::Any, rc::Rc};

use tulisp::{Error, ErrorKind, TulispContext, TulispObject};

pub(crate) fn add(ctx: &mut TulispContext) {
    ctx.add_function("dt:now", || TulispDateTime::from(chrono::Utc::now()));
    ctx.add_function("dt:minutes", |minutes: i64| {
        TulispTimeDelta::from(chrono::Duration::minutes(minutes))
    });
    ctx.add_function("dt:milliseconds", |milliseconds: i64| {
        TulispTimeDelta::from(chrono::Duration::milliseconds(milliseconds))
    });

    ctx.add_function(
        "dt+",
        |a: DateTimeTimeDelta, b: DateTimeTimeDelta| -> Result<TulispObject, Error> {
            match (a, b) {
                (DateTimeTimeDelta::DateTime(dt), DateTimeTimeDelta::TimeDelta(td))
                | (DateTimeTimeDelta::TimeDelta(td), DateTimeTimeDelta::DateTime(dt)) => {
                    Ok(TulispDateTime(dt.0 + td.0).into())
                }
                _ => Err(Error::new(
                    ErrorKind::TypeMismatch,
                    "dt+: Expected TulispDateTime + TulispTimeDelta".to_string(),
                )),
            }
        },
    );

    ctx.add_function(
        "dt-",
        |a: DateTimeTimeDelta, b: DateTimeTimeDelta| -> Result<TulispObject, Error> {
            match (a, b) {
                (DateTimeTimeDelta::DateTime(dt), DateTimeTimeDelta::TimeDelta(td)) => {
                    Ok(TulispDateTime(dt.0 - td.0).into())
                }
                (DateTimeTimeDelta::DateTime(dt1), DateTimeTimeDelta::DateTime(dt2)) => {
                    Ok(TulispTimeDelta(dt1.0 - dt2.0).into())
                }
                _ => Err(Error::new(
                    ErrorKind::TypeMismatch,
                    "dt-: Expected TulispDateTime - TulispTimeDelta or TulispDateTime - TulispDateTime"
                        .to_string(),
                )),
            }
        },
    );

    ctx.add_function(
        "dt:format",
        |timestamp: TulispDateTime, format: Option<String>| -> Result<String, Error> {
            Ok(timestamp
                .0
                .format(format.as_deref().unwrap_or("%Y-%m-%d %H:%M:%S%.f %:z"))
                .to_string())
        },
    );

    ctx.add_function("dt:dur->milliseconds", |delta: TulispTimeDelta| -> i64 {
        delta.0.num_milliseconds()
    });

    ctx.add_function(
        "dt:epoch-align",
        |timestamp: TulispDateTime, interval: TulispTimeDelta| -> TulispObject {
            epoch_align(timestamp.0, interval.0)
                .map(|dt| TulispDateTime(dt).into())
                .unwrap_or_else(|| false.into())
        },
    );
}

#[derive(Debug, Clone)]
struct TulispDateTime(chrono::DateTime<chrono::Utc>);

impl From<chrono::DateTime<chrono::Utc>> for TulispDateTime {
    fn from(value: chrono::DateTime<chrono::Utc>) -> Self {
        TulispDateTime(value)
    }
}

impl From<TulispDateTime> for TulispObject {
    fn from(value: TulispDateTime) -> Self {
        let rcany: Rc<dyn Any> = Rc::new(value);
        TulispObject::from(rcany)
    }
}

impl TryFrom<TulispObject> for TulispDateTime {
    type Error = Error;

    fn try_from(value: TulispObject) -> Result<Self, Self::Error> {
        match value.as_any() {
            Ok(value) => match value.downcast_ref::<TulispDateTime>() {
                Some(v) => Ok(v.clone()),
                None => Err(Error::new(
                    ErrorKind::TypeMismatch,
                    "Expected TulispDateTime".to_string(),
                )),
            },
            Err(_) => Err(Error::new(
                ErrorKind::TypeMismatch,
                "Expected TulispDateTime".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone)]
struct TulispTimeDelta(chrono::TimeDelta);

impl From<chrono::TimeDelta> for TulispTimeDelta {
    fn from(value: chrono::TimeDelta) -> Self {
        TulispTimeDelta(value)
    }
}

impl From<TulispTimeDelta> for TulispObject {
    fn from(value: TulispTimeDelta) -> Self {
        let rcany: Rc<dyn Any> = Rc::new(value);
        TulispObject::from(rcany)
    }
}

impl TryFrom<TulispObject> for TulispTimeDelta {
    type Error = Error;

    fn try_from(value: TulispObject) -> Result<Self, Self::Error> {
        match value.as_any() {
            Ok(value) => match value.downcast_ref::<TulispTimeDelta>() {
                Some(v) => Ok(v.clone()),
                None => Err(Error::new(
                    ErrorKind::TypeMismatch,
                    "Expected TulispTimeDelta".to_string(),
                )),
            },
            Err(_) => Err(Error::new(
                ErrorKind::TypeMismatch,
                "Expected TulispTimeDelta".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone)]
enum DateTimeTimeDelta {
    DateTime(TulispDateTime),
    TimeDelta(TulispTimeDelta),
}

impl TryFrom<TulispObject> for DateTimeTimeDelta {
    type Error = Error;

    fn try_from(value: TulispObject) -> Result<Self, Self::Error> {
        if let Ok(dt) = TulispDateTime::try_from(value.clone()) {
            Ok(DateTimeTimeDelta::DateTime(dt))
        } else if let Ok(td) = TulispTimeDelta::try_from(value.clone()) {
            Ok(DateTimeTimeDelta::TimeDelta(td))
        } else {
            Err(Error::new(
                ErrorKind::TypeMismatch,
                "Expected TulispDateTime or TulispTimeDelta".to_string(),
            ))
        }
    }
}

fn epoch_align(
    timestamp: chrono::DateTime<chrono::Utc>,
    interval: chrono::TimeDelta,
) -> Option<chrono::DateTime<chrono::Utc>> {
    let millis_since_epoch = timestamp.timestamp_millis();
    let interval_millis = interval.num_milliseconds();

    let intervals_since_epoch = millis_since_epoch / interval_millis;
    let aligned_millis_since_epoch = intervals_since_epoch * interval_millis;

    let aligned_timestamp = chrono::DateTime::from_timestamp_millis(aligned_millis_since_epoch)?;

    Some(aligned_timestamp)
}
