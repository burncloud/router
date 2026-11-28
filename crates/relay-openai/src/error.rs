use kernel::model::ModelId;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("no model is configured")]
    NoModels,
    #[error("model id is empty")]
    EmptyModel,
    #[error("max output tokens of {0:?} is zero")]
    ZeroMaxOutput(ModelId),
    #[error("model {0:?} is configured twice")]
    DuplicateModel(ModelId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PrepareError {
    #[error("request body is not a valid chat completions request")]
    InvalidBody,
    #[error("model is not available")]
    ModelNotFound,
    #[error("only the default service tier is supported")]
    UnsupportedServiceTier,
    #[error("parameter `{0}` is not supported")]
    UnsupportedParameter(&'static str),
    #[error("requested max output tokens exceed the model limit")]
    MaxTokensExceedsModelLimit,
    #[error("max_completion_tokens and max_tokens must not both be set")]
    ConflictingMaxTokens,
}

impl PrepareError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidBody => "invalid_request_body",
            Self::ModelNotFound => "model_not_found",
            Self::UnsupportedServiceTier => "unsupported_service_tier",
            Self::UnsupportedParameter(_) => "unsupported_parameter",
            Self::MaxTokensExceedsModelLimit => "max_tokens_exceeds_model_limit",
            Self::ConflictingMaxTokens => "conflicting_max_tokens",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MeterError {
    #[error("upstream event is not a JSON object")]
    InvalidEvent,
    #[error("upstream sent data after [DONE]")]
    DataAfterDone,
    #[error("upstream reported an error")]
    UpstreamError,
    #[error("upstream answered with a non-default service tier")]
    UnsupportedServiceTier,
    #[error("upstream sent usage twice")]
    DuplicateUsage,
    #[error("upstream sent no usage")]
    MissingUsage,
    #[error("usage does not match the expected shape")]
    InvalidUsage,
    #[error("total_tokens does not equal prompt_tokens plus completion_tokens")]
    InconsistentTotal,
    #[error("cached tokens exceed prompt tokens")]
    CachedExceedsPrompt,
    #[error("reasoning tokens exceed completion tokens")]
    ReasoningExceedsCompletion,
    #[error("usage contains unsupported billing item `{0}`")]
    UnsupportedBillingItem(&'static str),
    #[error("response body is not complete JSON")]
    IncompleteBody,
}
