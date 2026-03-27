use crate::logging;
use anyhow::{Context, Result, anyhow, bail};
use reqwest::Client;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Clone)]
pub struct TelegramClient {
    client: Client,
    base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramUpdate {
    pub update_id: i64,
    pub message: Option<TelegramMessage>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramMessage {
    pub message_id: i64,
    pub chat: TelegramChat,
    pub from: Option<TelegramUser>,
    pub text: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramChat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramUser {
    pub id: i64,
}

#[derive(Debug, Serialize)]
struct GetUpdatesRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<i64>,
    timeout: u64,
    limit: u32,
}

#[derive(Debug, Serialize)]
struct SendMessageRequest<'a> {
    chat_id: i64,
    text: &'a str,
}

#[derive(Debug, Serialize)]
struct EditMessageRequest<'a> {
    chat_id: i64,
    message_id: i64,
    text: &'a str,
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

impl TelegramClient {
    pub fn new(bot_token: String) -> Result<Self> {
        if bot_token.trim().is_empty() {
            bail!("empty Telegram bot token");
        }
        Ok(Self {
            client: Client::builder()
                .build()
                .context("failed to build reqwest client")?,
            base_url: format!("https://api.telegram.org/bot{bot_token}"),
        })
    }

    pub async fn get_updates(
        &self,
        offset: Option<i64>,
        timeout: u64,
        limit: u32,
    ) -> Result<Vec<TelegramUpdate>> {
        self.post(
            "getUpdates",
            &GetUpdatesRequest {
                offset,
                timeout,
                limit,
            },
        )
        .await
    }

    pub async fn send_message(&self, chat_id: i64, text: &str) -> Result<TelegramMessage> {
        logging::info(&format!(
            "telegram sendMessage chat_id={} text_len={}",
            chat_id,
            text.len()
        ));
        self.post("sendMessage", &SendMessageRequest { chat_id, text })
            .await
    }

    pub async fn edit_message(&self, chat_id: i64, message_id: i64, text: &str) -> Result<()> {
        logging::info(&format!(
            "telegram editMessageText chat_id={} message_id={} text_len={}",
            chat_id,
            message_id,
            text.len()
        ));
        let response = self
            .client
            .post(format!("{}/{}", self.base_url, "editMessageText"))
            .json(&EditMessageRequest {
                chat_id,
                message_id,
                text,
            })
            .send()
            .await
            .context("telegram editMessageText request failed")?;
        let status = response.status();
        let payload: ApiResponse<TelegramMessage> = response
            .json()
            .await
            .context("telegram editMessageText response decode failed")?;

        if !status.is_success() || !payload.ok {
            let description = payload
                .description
                .unwrap_or_else(|| format!("http {status}"));
            if is_telegram_message_not_modified(&description) {
                logging::info("telegram editMessageText noop: message already had requested text");
                return Ok(());
            }
            logging::error(&format!("telegram editMessageText failed: {description}"));
            return Err(anyhow!("telegram editMessageText failed: {description}"));
        }

        Ok(())
    }

    async fn post<T, B>(&self, method: &str, body: &B) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let response = self
            .client
            .post(format!("{}/{}", self.base_url, method))
            .json(body)
            .send()
            .await
            .with_context(|| format!("telegram {method} request failed"))?;
        let status = response.status();
        let payload: ApiResponse<T> = response
            .json()
            .await
            .with_context(|| format!("telegram {method} response decode failed"))?;
        if !status.is_success() || !payload.ok {
            let description = payload
                .description
                .clone()
                .unwrap_or_else(|| format!("http {status}"));
            if !(method == "getUpdates" && is_telegram_poll_conflict(&description)) {
                logging::error(&format!("telegram {method} failed: {description}"));
            }
            return Err(anyhow!(
                "telegram {method} failed: {}",
                payload
                    .description
                    .unwrap_or_else(|| format!("http {status}"))
            ));
        }
        payload
            .result
            .ok_or_else(|| anyhow!("telegram {method} returned no result"))
    }
}

pub(crate) fn is_telegram_message_not_modified(error_text: &str) -> bool {
    error_text
        .to_ascii_lowercase()
        .contains("message is not modified")
}

pub(crate) fn is_telegram_poll_conflict(error_text: &str) -> bool {
    error_text
        .to_ascii_lowercase()
        .contains("terminated by other getupdates request")
}

#[cfg(test)]
mod tests {
    use super::{is_telegram_message_not_modified, is_telegram_poll_conflict};

    #[test]
    fn detects_message_not_modified_error() {
        assert!(is_telegram_message_not_modified(
            "Bad Request: message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message"
        ));
    }

    #[test]
    fn detects_poll_conflict_error() {
        assert!(is_telegram_poll_conflict(
            "telegram getUpdates failed: Conflict: terminated by other getUpdates request; make sure that only one bot instance is running"
        ));
    }
}
