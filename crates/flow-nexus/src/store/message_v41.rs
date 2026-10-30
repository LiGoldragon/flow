//! Exact mirrors of the persisted message compositions through Flow 0.41.
//!
//! These types are deliberately local and frozen.  Current message types
//! gained a typed answerable on Result in 0.42; every Memory predecessor that
//! embedded a Request must continue decoding the earlier archive before the
//! family upgrade can carry it forward.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskV41 {
    pub(crate) minutes: i64,
    pub(crate) string: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct DoneV41 {
    pub(crate) logical_delivery_id: String,
    pub(crate) string: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum RequestV41 {
    Order(String),
    Question(String),
    Psyche(flow_ethos::Said),
    Psyches(Vec<flow_ethos::Said>),
    Result(String),
    Notice(String),
    Task(TaskV41),
    Done(DoneV41),
}

impl From<RequestV41> for flow_ethos::Request {
    fn from(previous: RequestV41) -> Self {
        match previous {
            RequestV41::Order(value) => Self::Order(value),
            RequestV41::Question(value) => Self::Question(value),
            RequestV41::Psyche(value) => Self::Psyche(value),
            RequestV41::Psyches(value) => Self::Psyches(value),
            RequestV41::Result(value) => Self::Result(flow_ethos::ResultPayload::Historical(value)),
            RequestV41::Notice(value) => Self::Notice(value),
            RequestV41::Task(value) => Self::Task(flow_ethos::AnswerableTask {
                minutes: value.minutes,
                string: value.string,
            }),
            RequestV41::Done(value) => Self::Done(flow_ethos::DonePayload::Historical(
                flow_ethos::Historical_Data {
                    logical_delivery_id: value.logical_delivery_id,
                    string: value.string,
                },
            )),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum MessageV41 {
    HardAbrupt(RequestV41),
    MiddleAbrupt(RequestV41),
    Soft(RequestV41),
}

impl From<MessageV41> for flow_ethos::Message {
    fn from(previous: MessageV41) -> Self {
        match previous {
            MessageV41::HardAbrupt(value) => Self::HardAbrupt(value.into()),
            MessageV41::MiddleAbrupt(value) => Self::MiddleAbrupt(value.into()),
            MessageV41::Soft(value) => Self::Soft(value.into()),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueueEntryV41 {
    pub(crate) logical_delivery_id: String,
    pub(crate) sent_by: flow_ethos::SentBy,
    pub(crate) delivery: flow_ethos::Delivery,
    pub(crate) message: MessageV41,
}

impl From<QueueEntryV41> for flow_ethos::QueueEntry {
    fn from(previous: QueueEntryV41) -> Self {
        Self {
            logical_delivery_id: previous.logical_delivery_id,
            sent_by: previous.sent_by,
            delivery: previous.delivery,
            message: previous.message.into(),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetaflowV41 {
    pub(crate) address: flow_ethos::Address,
    pub(crate) state: flow_ethos::State,
    pub(crate) past: Vec<String>,
    pub(crate) queue: Vec<QueueEntryV41>,
}

impl From<MetaflowV41> for flow_ethos::Metaflow {
    fn from(previous: MetaflowV41) -> Self {
        Self {
            address: previous.address,
            state: previous.state,
            past: previous.past,
            queue: previous.queue.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyEvidenceV41 {
    pub(crate) sent_by: flow_ethos::SentBy,
    pub(crate) request: RequestV41,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueueEvidenceV41 {
    Logical(QueueEntryV41),
    Legacy(LegacyEvidenceV41),
}

impl From<QueueEvidenceV41> for meta_signal_flow::QueueEvidence {
    fn from(previous: QueueEvidenceV41) -> Self {
        match previous {
            QueueEvidenceV41::Logical(value) => Self::Logical(value.into()),
            QueueEvidenceV41::Legacy(value) => {
                Self::Legacy(meta_signal_flow::QueueEvidence_Legacy_Data {
                    sent_by: value.sent_by,
                    request: value.request.into(),
                })
            }
        }
    }
}
