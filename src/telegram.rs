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
        self.post("sendMessage", &SendMessageRequest { chat_id, text })
            .await
    }

    pub async fn edit_message(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
    ) -> Result<TelegramMessage> {
        self.post(
            "editMessageText",
            &EditMessageRequest {
                chat_id,
                message_id,
                text,
            },
        )
        .await
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
