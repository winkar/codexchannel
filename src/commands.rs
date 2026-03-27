#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BotCommand {
    Start,
    New,
    Use(String),
    Status,
    Stop,
    Prompt(String),
    Invalid(String),
}

impl BotCommand {
    pub fn parse(input: &str) -> Self {
        let trimmed = input.trim();
        if !trimmed.starts_with('/') {
            return Self::Prompt(trimmed.to_string());
        }

        let mut parts = trimmed.splitn(2, char::is_whitespace);
        let command = parts.next().unwrap_or_default();
        let rest = parts.next().unwrap_or("").trim();
        match command {
            "/start" => Self::Start,
            "/new" => Self::New,
            "/status" => Self::Status,
            "/stop" => Self::Stop,
            "/use" => {
                if rest.is_empty() {
                    Self::Invalid(Self::help())
                } else {
                    Self::Use(rest.to_string())
                }
            }
            _ => Self::Invalid(Self::help()),
        }
    }

    pub fn help() -> String {
        "/new\n/use <thread_id>\n/status\n/stop".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::BotCommand;

    #[test]
    fn parses_new_command() {
        assert_eq!(BotCommand::parse("/new"), BotCommand::New);
    }

    #[test]
    fn parses_use_command() {
        assert_eq!(
            BotCommand::parse("/use thread-123"),
            BotCommand::Use("thread-123".to_string())
        );
    }

    #[test]
    fn parses_prompt() {
        assert_eq!(
            BotCommand::parse("hello"),
            BotCommand::Prompt("hello".to_string())
        );
    }

    #[test]
    fn invalid_use_without_arg() {
        match BotCommand::parse("/use") {
            BotCommand::Invalid(_) => {}
            other => panic!("expected invalid, got {other:?}"),
        }
    }
}
