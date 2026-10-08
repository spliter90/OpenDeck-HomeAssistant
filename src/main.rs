use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures_util::{SinkExt, StreamExt};
use openaction::*;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{broadcast, Mutex},
    task::JoinHandle,
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const ENTITY_ACTION_UUID: &str = "de.spliter90.homeassistant.entity";
const SERVICE_ACTION_UUID: &str = "de.spliter90.homeassistant.service";

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
struct EntitySettings {
    base_url: String,
    token: String,
    entity_id: String,
    press_action: String,
    display_attribute: String,
    label: String,
    icon: String,
}

impl Default for EntitySettings {
    fn default() -> Self {
        Self {
            base_url: "http://homeassistant.local:8123".into(),
            token: String::new(),
            entity_id: String::new(),
            press_action: "toggle".into(),
            display_attribute: String::new(),
            label: String::new(),
            icon: "auto".into(),
        }
    }
}

impl EntitySettings {
    fn valid(&self) -> bool {
        valid_server(&self.base_url, &self.token) && !self.entity_id.trim().is_empty()
    }

    fn server_key(&self) -> ServerKey {
        ServerKey::new(&self.base_url, &self.token)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
struct ServiceSettings {
    base_url: String,
    token: String,
    domain: String,
    service: String,
    service_data: String,
    label: String,
    preset: String,
    target_entity_id: String,
    icon: String,
    automation_skip_condition: bool,
}

impl Default for ServiceSettings {
    fn default() -> Self {
        Self {
            base_url: "http://homeassistant.local:8123".into(),
            token: String::new(),
            domain: String::new(),
            service: String::new(),
            service_data: "{}".into(),
            label: "HA ACTION".into(),
            preset: "manual".into(),
            target_entity_id: String::new(),
            icon: "auto".into(),
            automation_skip_condition: true,
        }
    }
}

impl ServiceSettings {
    fn valid(&self) -> bool {
        if !valid_server(&self.base_url, &self.token) {
            return false;
        }
        match self.preset.as_str() {
            "scene" | "script" | "automation" => !self.target_entity_id.trim().is_empty(),
            _ => !self.domain.trim().is_empty() && !self.service.trim().is_empty(),
        }
    }
}

fn valid_server(base_url: &str, token: &str) -> bool {
    let base = base_url.trim();
    (base.starts_with("http://") || base.starts_with("https://")) && !token.trim().is_empty()
}

#[derive(Clone, Eq)]
struct ServerKey {
    base_url: String,
    token: String,
}

impl ServerKey {
    fn new(base_url: &str, token: &str) -> Self {
        Self {
            base_url: base_url.trim().trim_end_matches('/').to_owned(),
            token: token.trim().to_owned(),
        }
    }
}

impl PartialEq for ServerKey {
    fn eq(&self, other: &Self) -> bool {
        self.base_url == other.base_url && self.token == other.token
    }
}

impl Hash for ServerKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.base_url.hash(state);
        self.token.hash(state);
    }
}

#[derive(Clone, Debug)]
enum HaEvent {
    Connected(bool),
    State(HaState),
}

#[derive(Clone, Debug, Deserialize)]
struct HaState {
    entity_id: String,
    state: String,
    #[serde(default)]
    attributes: Map<String, Value>,
}

struct ConnectionEntry {
    sender: broadcast::Sender<HaEvent>,
    refs: usize,
    task: JoinHandle<()>,
}

struct EntityBinding {
    key: ServerKey,
    settings: EntitySettings,
    render_task: JoinHandle<()>,
}

#[derive(Default)]
struct SharedState {
    connections: Mutex<HashMap<ServerKey, ConnectionEntry>>,
    entity_bindings: Mutex<HashMap<String, EntityBinding>>,
}

impl SharedState {
    async fn bind_entity(&self, instance: &Instance, settings: &EntitySettings) -> OpenActionResult<()> {
        self.unbind_entity(&instance.instance_id).await;

        if !settings.valid() {
            render_setup_missing(instance).await?;
            return Ok(());
        }

        instance
            .set_image(Some(tile_background_data_url("#424242", "house")), None)
            .await?;
        instance.set_title(Some("VERBINDE..."), None).await?;

        let key = settings.server_key();
        let mut receiver = {
            let mut connections = self.connections.lock().await;
            if let Some(entry) = connections.get_mut(&key) {
                entry.refs += 1;
                entry.sender.subscribe()
            } else {
                let (sender, receiver) = broadcast::channel(256);
                let worker_sender = sender.clone();
                let worker_key = key.clone();
                let task = tokio::spawn(async move {
                    websocket_worker(worker_key, worker_sender).await;
                });
                connections.insert(
                    key.clone(),
                    ConnectionEntry {
                        sender,
                        refs: 1,
                        task,
                    },
                );
                receiver
            }
        };

        let instance_id = instance.instance_id.clone();
        let render_settings = settings.clone();
        let render_key = key.clone();
        let render_task = tokio::spawn(async move {
            let mut current_state = match fetch_state(&render_key, &render_settings.entity_id).await {
                Ok(state) => Some(state),
                Err(error) => {
                    log::warn!("Initial Home Assistant state fetch failed: {error}");
                    None
                }
            };

            render_entity_instance(&instance_id, &render_settings, current_state.as_ref(), true).await;

            loop {
                match receiver.recv().await {
                    Ok(HaEvent::State(state)) if state.entity_id == render_settings.entity_id => {
                        current_state = Some(state);
                        render_entity_instance(&instance_id, &render_settings, current_state.as_ref(), true).await;
                    }
                    Ok(HaEvent::Connected(connected)) => {
                        render_entity_instance(
                            &instance_id,
                            &render_settings,
                            current_state.as_ref(),
                            connected,
                        )
                        .await;
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if let Ok(state) = fetch_state(&render_key, &render_settings.entity_id).await {
                            current_state = Some(state);
                            render_entity_instance(
                                &instance_id,
                                &render_settings,
                                current_state.as_ref(),
                                true,
                            )
                            .await;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        self.entity_bindings.lock().await.insert(
            instance.instance_id.clone(),
            EntityBinding {
                key,
                settings: settings.clone(),
                render_task,
            },
        );

        Ok(())
    }

    async fn unbind_entity(&self, instance_id: &str) {
        let binding = self.entity_bindings.lock().await.remove(instance_id);
        let Some(binding) = binding else {
            return;
        };

        binding.render_task.abort();

        let mut connections = self.connections.lock().await;
        let should_remove = if let Some(entry) = connections.get_mut(&binding.key) {
            entry.refs = entry.refs.saturating_sub(1);
            entry.refs == 0
        } else {
            false
        };

        if should_remove {
            if let Some(entry) = connections.remove(&binding.key) {
                entry.task.abort();
            }
        }
    }

    async fn run_entity_press(&self, instance_id: &str) -> bool {
        let settings = {
            let bindings = self.entity_bindings.lock().await;
            let Some(binding) = bindings.get(instance_id) else {
                return false;
            };
            binding.settings.clone()
        };

        match settings.press_action.as_str() {
            "none" => true,
            "toggle" | "turn_on" | "turn_off" => {
                let key = settings.server_key();
                let payload = json!({"entity_id": settings.entity_id});
                call_service(&key, "homeassistant", &settings.press_action, payload)
                    .await
                    .is_ok()
            }
            _ => false,
        }
    }
}

struct EntityAction {
    shared: Arc<SharedState>,
}

#[async_trait]
impl Action for EntityAction {
    const UUID: ActionUuid = ENTITY_ACTION_UUID;
    type Settings = EntitySettings;

    async fn will_appear(&self, instance: &Instance, settings: &Self::Settings) -> OpenActionResult<()> {
        self.shared.bind_entity(instance, settings).await
    }

    async fn did_receive_settings(&self, instance: &Instance, settings: &Self::Settings) -> OpenActionResult<()> {
        self.shared.bind_entity(instance, settings).await
    }

    async fn key_up(&self, instance: &Instance, _settings: &Self::Settings) -> OpenActionResult<()> {
        if self.shared.run_entity_press(&instance.instance_id).await {
            instance.show_ok().await?;
        } else {
            instance.show_alert().await?;
        }
        Ok(())
    }

    async fn will_disappear(&self, instance: &Instance, _settings: &Self::Settings) -> OpenActionResult<()> {
        self.shared.unbind_entity(&instance.instance_id).await;
        Ok(())
    }
}

struct ServiceAction;

#[async_trait]
impl Action for ServiceAction {
    const UUID: ActionUuid = SERVICE_ACTION_UUID;
    type Settings = ServiceSettings;

    async fn will_appear(&self, instance: &Instance, settings: &Self::Settings) -> OpenActionResult<()> {
        render_service_tile(instance, settings).await
    }

    async fn did_receive_settings(&self, instance: &Instance, settings: &Self::Settings) -> OpenActionResult<()> {
        render_service_tile(instance, settings).await
    }

    async fn key_up(&self, instance: &Instance, settings: &Self::Settings) -> OpenActionResult<()> {
        if !settings.valid() {
            instance.show_alert().await?;
            return Ok(());
        }

        let (domain, service, data) = match resolve_service_call(settings) {
            Ok(call) => call,
            Err(error) => {
                log::warn!("Invalid Home Assistant action settings: {error}");
                instance.show_alert().await?;
                return Ok(());
            }
        };

        let key = ServerKey::new(&settings.base_url, &settings.token);
        match call_service(&key, domain, service, data).await {
            Ok(_) => instance.show_ok().await?,
            Err(error) => {
                log::warn!("Home Assistant service call failed: {error}");
                instance.show_alert().await?;
            }
        }
        Ok(())
    }
}

async fn render_setup_missing(instance: &Instance) -> OpenActionResult<()> {
    instance
        .set_image(Some(tile_background_data_url("#424242", "house")), None)
        .await?;
    instance.set_title(Some("HA\nSETUP"), None).await?;
    Ok(())
}

async fn render_service_tile(instance: &Instance, settings: &ServiceSettings) -> OpenActionResult<()> {
    let valid = settings.valid() && resolve_service_call(settings).is_ok();
    let color = if !valid {
        "#424242"
    } else {
        match settings.preset.as_str() {
            "scene" => "#6A1B9A",
            "automation" => "#EF6C00",
            "script" => "#1565C0",
            _ => "#1565C0",
        }
    };

    let title = if settings.label.trim().is_empty() {
        if matches!(settings.preset.as_str(), "scene" | "script" | "automation") {
            if settings.target_entity_id.trim().is_empty() {
                "HA\nACTION".to_owned()
            } else {
                humanize_entity_id(&settings.target_entity_id)
            }
        } else if settings.domain.trim().is_empty() || settings.service.trim().is_empty() {
            "HA\nACTION".to_owned()
        } else {
            format!("{}\n{}", settings.domain.trim(), settings.service.trim())
        }
    } else {
        settings.label.trim().to_owned()
    };

    let icon = service_icon(settings);
    instance
        .set_image(Some(tile_background_data_url(color, icon)), None)
        .await?;
    instance.set_title(Some(title), None).await?;
    Ok(())
}

fn service_icon(settings: &ServiceSettings) -> &str {
    if settings.icon.trim().is_empty() || settings.icon == "auto" {
        match settings.preset.as_str() {
            "scene" => "scene",
            "script" => "routine",
            "automation" => "automation",
            _ => "play",
        }
    } else {
        settings.icon.as_str()
    }
}

fn resolve_service_call(settings: &ServiceSettings) -> Result<(&str, &str, Value), String> {
    match settings.preset.as_str() {
        "scene" => Ok((
            "scene",
            "turn_on",
            json!({"entity_id": settings.target_entity_id.trim()}),
        )),
        "script" => Ok((
            "script",
            "turn_on",
            json!({"entity_id": settings.target_entity_id.trim()}),
        )),
        "automation" => Ok((
            "automation",
            "trigger",
            json!({
                "entity_id": settings.target_entity_id.trim(),
                "skip_condition": settings.automation_skip_condition
            }),
        )),
        _ => parse_service_data(&settings.service_data)
            .map(|data| (settings.domain.as_str(), settings.service.as_str(), data))
            .map_err(|error| error.to_string()),
    }
}

async fn render_entity_instance(
    instance_id: &str,
    settings: &EntitySettings,
    state: Option<&HaState>,
    websocket_connected: bool,
) {
    let Some(instance) = get_instance(instance_id.to_owned()).await else {
        return;
    };

    let (title, color) = match state {
        Some(state) => (
            format_entity_title(settings, state),
            entity_background_color(state, websocket_connected),
        ),
        None if websocket_connected => ("HA\nNICHT GEF.".into(), "#C62828"),
        None => ("HA\nOFFLINE".into(), "#424242"),
    };

    if let Err(error) = instance
        .set_image(Some(tile_background_data_url(color, entity_icon(settings, state))), None)
        .await
    {
        log::warn!("Could not update Home Assistant image: {error}");
    }
    if let Err(error) = instance.set_title(Some(title), None).await {
        log::warn!("Could not update Home Assistant title: {error}");
    }
}

fn format_entity_title(settings: &EntitySettings, state: &HaState) -> String {
    let label = if !settings.label.trim().is_empty() {
        settings.label.trim().to_owned()
    } else {
        state
            .attributes
            .get("friendly_name")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| humanize_entity_id(&state.entity_id))
    };

    let value = if !settings.display_attribute.trim().is_empty() {
        let key = settings.display_attribute.trim();
        state
            .attributes
            .get(key)
            .map(format_json_value)
            .unwrap_or_else(|| "—".into())
    } else {
        state.state.clone()
    };

    let value = if settings.display_attribute.trim().is_empty() {
        if let Some(unit) = state
            .attributes
            .get("unit_of_measurement")
            .and_then(Value::as_str)
        {
            format!("{value} {unit}")
        } else {
            value
        }
    } else {
        value
    };

    let mut label = label;
    if label.chars().count() > 18 {
        label = label.chars().take(18).collect();
    }

    format!("{}\n{}", label, value)
}

fn format_json_value(value: &Value) -> String {
    match value {
        Value::Null => "—".into(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_else(|_| "—".into()),
    }
}

fn humanize_entity_id(entity_id: &str) -> String {
    let object = entity_id.split_once('.').map(|(_, object)| object).unwrap_or(entity_id);
    object
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn entity_background_color(state: &HaState, websocket_connected: bool) -> &'static str {
    if !websocket_connected {
        return "#424242";
    }

    match state.state.to_ascii_lowercase().as_str() {
        "unavailable" => "#C62828",
        "unknown" => "#616161",
        "on" | "home" | "open" | "playing" | "heating" | "cooling" => "#2E7D32",
        "off" | "not_home" | "closed" | "idle" | "paused" => "#424242",
        _ => "#1565C0",
    }
}

fn entity_icon<'a>(settings: &'a EntitySettings, state: &HaState) -> &'a str {
    if !settings.icon.trim().is_empty() && settings.icon != "auto" {
        return settings.icon.as_str();
    }
    match state.entity_id.split_once('.').map(|(domain, _)| domain) {
        Some("light") => "bulb",
        Some("switch") => "switch",
        Some("sensor") | Some("binary_sensor") => "sensor",
        Some("climate") => "climate",
        Some("scene") => "scene",
        Some("script") => "routine",
        Some("automation") => "automation",
        _ => "house",
    }
}

fn tile_background_data_url(color: &str, icon: &str) -> String {
    let icon_svg = icon_svg(icon);
    let svg = format!(
        r#"<svg xmlns="http://www.w3.org/2000/svg" width="144" height="144" viewBox="0 0 144 144"><rect width="144" height="144" rx="18" fill="{color}"/><rect x="5" y="5" width="134" height="134" rx="14" fill="none" stroke="white" stroke-opacity=".14" stroke-width="2"/><g transform="translate(38 14)" fill="none" stroke="white" stroke-width="5" stroke-linecap="round" stroke-linejoin="round" opacity=".38">{icon_svg}</g></svg>"#
    );
    format!("data:image/svg+xml;base64,{}", BASE64.encode(svg.as_bytes()))
}

fn icon_svg(icon: &str) -> &'static str {
    match icon {
        "bulb" => r#"<path d="M34 4c-14 0-25 11-25 25 0 9 5 17 12 22v9h26v-9c7-5 12-13 12-22C59 15 48 4 34 4Z"/><path d="M23 68h22M26 76h16"/>"#,
        "ceiling" => r#"<path d="M34 3v14M18 18h32l8 18H10l8-18Z"/><path d="M14 43h40M20 51h28"/>"#,
        "pendant" => r#"<path d="M34 2v25M18 27h32l8 20H10l8-20Z"/><path d="M17 55h34"/>"#,
        "desk" => r#"<path d="M12 70h45M24 70l9-27M33 43l15-13M44 24l13 13-9 9-13-13 9-9Z"/>"#,
        "floor" => r#"<path d="M34 8v55M18 8h32l7 18H11l7-18Z"/><path d="M21 72h26M34 63v9"/>"#,
        "spot" => r#"<path d="M12 16h23l12 12-20 20-15-9V16Z"/><path d="M36 43 56 63M45 35l15 8M31 49l8 15"/>"#,
        "strip" => r#"<path d="M8 22h52v30H8z"/><path d="M16 30v14M26 30v14M36 30v14M46 30v14M56 30v14"/>"#,
        "outdoor" => r#"<path d="M20 8h28v20H20zM16 28h36v38H16z"/><path d="M24 36h20v20H24zM34 66v10"/>"#,
        "switch" => r#"<rect x="14" y="10" width="40" height="60" rx="8"/><circle cx="34" cy="31" r="9"/><path d="M34 22v18"/>"#,
        "sensor" => r#"<circle cx="34" cy="38" r="6"/><path d="M20 24a20 20 0 0 0 0 28M48 24a20 20 0 0 1 0 28M12 16a31 31 0 0 0 0 44M56 16a31 31 0 0 1 0 44"/>"#,
        "climate" => r#"<path d="M30 8v38a13 13 0 1 0 8 0V8a4 4 0 0 0-8 0Z"/><path d="M34 20v34"/>"#,
        "scene" => r#"<path d="M8 58 24 40l12 10 12-20 12 28H8Z"/><circle cx="21" cy="19" r="7"/>"#,
        "routine" => r#"<path d="M12 22h33M12 38h45M12 54h29"/><path d="m49 51 9 9-9 9"/>"#,
        "automation" => r#"<circle cx="34" cy="38" r="17"/><path d="M34 8v9M34 59v9M4 38h9M55 38h9M13 17l7 7M48 52l7 7M55 17l-7 7M20 52l-7 7"/>"#,
        "play" => r#"<circle cx="34" cy="38" r="28"/><path d="m28 24 22 14-22 14V24Z"/>"#,
        _ => r#"<path d="M7 34 34 10l27 24v32H43V45H25v21H7V34Z"/>"#,
    }
}

async fn fetch_state(key: &ServerKey, entity_id: &str) -> Result<HaState, String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|e| e.to_string())?;
    let url = format!("{}/api/states/{}", key.base_url, entity_id.trim());
    let response = client
        .get(url)
        .bearer_auth(&key.token)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }

    response.json::<HaState>().await.map_err(|e| e.to_string())
}

async fn call_service(
    key: &ServerKey,
    domain: &str,
    service: &str,
    data: Value,
) -> Result<Value, String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(12))
        .build()
        .map_err(|e| e.to_string())?;
    let url = format!(
        "{}/api/services/{}/{}",
        key.base_url,
        domain.trim(),
        service.trim()
    );
    let response = client
        .post(url)
        .bearer_auth(&key.token)
        .json(&data)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let status = response.status();
    let body = response.text().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        return Err(format!("HTTP {status}: {body}"));
    }
    if body.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&body).or(Ok(Value::String(body)))
}

fn parse_service_data(raw: &str) -> Result<Value, serde_json::Error> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        Ok(json!({}))
    } else {
        serde_json::from_str(trimmed)
    }
}

fn websocket_url(base_url: &str) -> Result<String, String> {
    let base = base_url.trim().trim_end_matches('/');
    if let Some(rest) = base.strip_prefix("http://") {
        Ok(format!("ws://{rest}/api/websocket"))
    } else if let Some(rest) = base.strip_prefix("https://") {
        Ok(format!("wss://{rest}/api/websocket"))
    } else {
        Err("Home Assistant URL must start with http:// or https://".into())
    }
}

async fn websocket_worker(key: ServerKey, sender: broadcast::Sender<HaEvent>) {
    loop {
        let _ = sender.send(HaEvent::Connected(false));
        match run_websocket_session(&key, &sender).await {
            Ok(()) => {}
            Err(error) => log::warn!("Home Assistant WebSocket disconnected: {error}"),
        }
        let _ = sender.send(HaEvent::Connected(false));
        sleep(Duration::from_secs(5)).await;
    }
}

async fn run_websocket_session(
    key: &ServerKey,
    sender: &broadcast::Sender<HaEvent>,
) -> Result<(), String> {
    let ws_url = websocket_url(&key.base_url)?;
    let (socket, _) = connect_async(&ws_url).await.map_err(|e| e.to_string())?;
    let (mut write, mut read) = socket.split();

    let first = read
        .next()
        .await
        .ok_or_else(|| "WebSocket closed before authentication".to_owned())?
        .map_err(|e| e.to_string())?;
    let first_json = parse_ws_json(first)?;
    if first_json.get("type").and_then(Value::as_str) != Some("auth_required") {
        return Err("Unexpected Home Assistant WebSocket greeting".into());
    }

    write
        .send(Message::Text(
            json!({"type": "auth", "access_token": key.token}).to_string().into(),
        ))
        .await
        .map_err(|e| e.to_string())?;

    let auth = read
        .next()
        .await
        .ok_or_else(|| "WebSocket closed during authentication".to_owned())?
        .map_err(|e| e.to_string())?;
    let auth_json = parse_ws_json(auth)?;
    if auth_json.get("type").and_then(Value::as_str) != Some("auth_ok") {
        return Err(format!("Home Assistant WebSocket authentication failed: {auth_json}"));
    }

    write
        .send(Message::Text(
            json!({"id": 1, "type": "subscribe_events", "event_type": "state_changed"})
                .to_string()
                .into(),
        ))
        .await
        .map_err(|e| e.to_string())?;

    let _ = sender.send(HaEvent::Connected(true));

    while let Some(message) = read.next().await {
        let message = message.map_err(|e| e.to_string())?;
        let value = match parse_ws_json(message) {
            Ok(value) => value,
            Err(_) => continue,
        };

        if value.get("type").and_then(Value::as_str) != Some("event") {
            continue;
        }
        if value
            .pointer("/event/event_type")
            .and_then(Value::as_str)
            != Some("state_changed")
        {
            continue;
        }

        let Some(new_state) = value.pointer("/event/data/new_state") else {
            continue;
        };
        if new_state.is_null() {
            continue;
        }

        if let Ok(state) = serde_json::from_value::<HaState>(new_state.clone()) {
            let _ = sender.send(HaEvent::State(state));
        }
    }

    Err("WebSocket stream ended".into())
}

fn parse_ws_json(message: Message) -> Result<Value, String> {
    match message {
        Message::Text(text) => serde_json::from_str(text.as_str()).map_err(|e| e.to_string()),
        Message::Binary(data) => serde_json::from_slice(&data).map_err(|e| e.to_string()),
        Message::Close(_) => Err("WebSocket closed".into()),
        _ => Err("Unsupported WebSocket message".into()),
    }
}

#[tokio::main]
async fn main() -> OpenActionResult<()> {
    use simplelog::*;
    if let Err(error) = TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Stdout,
        ColorChoice::Never,
    ) {
        eprintln!("Logger initialization failed: {error}");
    }

    let shared = Arc::new(SharedState::default());
    register_action(EntityAction {
        shared: shared.clone(),
    })
    .await;
    register_action(ServiceAction).await;
    run(std::env::args().collect()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_websocket_urls() {
        assert_eq!(
            websocket_url("http://homeassistant.local:8123").unwrap(),
            "ws://homeassistant.local:8123/api/websocket"
        );
        assert_eq!(
            websocket_url("https://ha.example.com/").unwrap(),
            "wss://ha.example.com/api/websocket"
        );
    }

    #[test]
    fn parses_empty_service_data() {
        assert_eq!(parse_service_data("  ").unwrap(), json!({}));
    }

    #[test]
    fn formats_sensor_with_unit() {
        let settings = EntitySettings {
            entity_id: "sensor.wohnzimmer_temperatur".into(),
            label: "Wohnzimmer".into(),
            ..Default::default()
        };
        let state = HaState {
            entity_id: settings.entity_id.clone(),
            state: "21.7".into(),
            attributes: Map::from_iter([
                ("friendly_name".into(), Value::String("Temperatur".into())),
                ("unit_of_measurement".into(), Value::String("°C".into())),
            ]),
        };
        assert_eq!(format_entity_title(&settings, &state), "Wohnzimmer\n21.7 °C");
    }

    #[test]
    fn maps_entity_colors() {
        let on = HaState {
            entity_id: "switch.test".into(),
            state: "on".into(),
            attributes: Map::new(),
        };
        let unavailable = HaState {
            entity_id: "switch.test".into(),
            state: "unavailable".into(),
            attributes: Map::new(),
        };
        assert_eq!(entity_background_color(&on, true), "#2E7D32");
        assert_eq!(entity_background_color(&unavailable, true), "#C62828");
        assert_eq!(entity_background_color(&on, false), "#424242");
    }
}
