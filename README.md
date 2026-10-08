# Home Assistant for OpenDeck

OpenAction/OpenDeck plugin for Home Assistant. It shows live Home Assistant entity states on OpenDeck keys and can trigger generic entity controls or arbitrary Home Assistant actions/services.

## Features

- Live entity updates through the Home Assistant WebSocket API (`/api/websocket`)
- Initial state fetch through the REST API (`/api/states/<entity_id>`)
- Generic entity controls: toggle, turn on, turn off, or display-only
- Selectable key symbols: bulb, ceiling light, pendant, desk lamp, floor lamp, spot, LED strip, outdoor light and more
- One-click presets for Home Assistant scenes, scripts/routines and automation triggers
- Arbitrary Home Assistant action/service calls through `/api/services/<domain>/<service>`
- Optional entity attribute display, e.g. `current_temperature`, `brightness`, or `battery_level`
- Dynamic key colors: green for active/on states, blue for other values, red for unavailable, grey for off/offline
- Windows, macOS and Linux build targets
- No Home Assistant add-on required

## Setup

1. In Home Assistant, open your user profile and create a **Long-Lived Access Token**.
2. Install the plugin in OpenDeck.
3. Add either **Home Assistant → Entity** or **Home Assistant → Action / Service** to a key.
4. Set the Home Assistant URL, for example `http://homeassistant.local:8123`.
5. Paste the token.

### Entity examples

- `light.wohnzimmer` + **Umschalten**
- `switch.kaffeemaschine` + **Umschalten**
- `sensor.wohnzimmer_temperatur` + **Nur anzeigen**
- `climate.wohnzimmer` + attribute `current_temperature`


### Scene, routine and automation presets

The **Action / Service** key now has presets, so no JSON is required for common workflows:

- **Scene** → enter e.g. `scene.fernsehabend`
- **Script / Routine** → enter e.g. `script.gute_nacht`
- **Automation** → enter e.g. `automation.abendroutine`; choose whether Home Assistant conditions should be skipped or checked

For lights and other entities, choose a dedicated symbol in the Property Inspector. With **Auto**, light entities automatically use the bulb icon.

### Action / Service examples

Scene:

```text
Domain: scene
Service: turn_on
JSON: {"entity_id":"scene.fernsehen"}
```

Script:

```text
Domain: script
Service: turn_on
JSON: {"entity_id":"script.gute_nacht"}
```

Light at 35%:

```text
Domain: light
Service: turn_on
JSON: {"entity_id":"light.wohnzimmer","brightness_pct":35}
```

## Home Assistant API

Home Assistant already exposes the required APIs. This plugin talks directly to them and therefore does not require a custom Home Assistant integration or an additional bridge service.

Authentication uses an `Authorization: Bearer <token>` header for REST and the normal Home Assistant WebSocket authentication handshake.

## Security

The Long-Lived Access Token grants the permissions of the Home Assistant user that created it. Create a dedicated Home Assistant user for OpenDeck if you want to keep permissions and revocation separate. Do not commit tokens to Git.

## Development

```bash
cargo test
cargo build --release
```

The project uses the official `openaction` Rust crate.

## License

MIT
