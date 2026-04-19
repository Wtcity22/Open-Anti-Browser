<p align="center">
  <img src="./frontend/public/first.png" alt="Open-Anti-Browser" width="100%">
</p>

# Open-Anti-Browser

[中文说明](./README.md)

Open-Anti-Browser is a local desktop manager for fingerprint browser profiles

It brings two publicly available fingerprint browser engines into one interface for profile creation, proxy management, extension management, browser launching, and local API automation

The interface follows an iOS-inspired style and supports light and dark mode switching

<p align="center">
  <img src="./frontend/public/banner1.png" alt="Open-Anti-Browser banner 1" width="100%">
</p>

<p align="center">
  <img src="./frontend/public/banner2.png" alt="Open-Anti-Browser banner 2" width="100%">
</p>

## Download

- Installer release page: [Releases](https://github.com/Wtcity22/Open-Anti-Browser/releases)
- Source repository: [Wtcity22/Open-Anti-Browser](https://github.com/Wtcity22/Open-Anti-Browser)

## Contact Me
- email: wtcity22@gmail.com
- tg: @NetOriginDev

## Community group

You are welcome to join the QQ group to share usage tips, report issues, and exchange ideas with other users

<p align="center">
  <img src="./frontend/public/qq.jpg" alt="Open-Anti-Browser QQ group" width="260">
</p>

## Engine sources

### Chromium 144

- Project: [adryfish/fingerprint-chromium](https://github.com/adryfish/fingerprint-chromium)
- Bundled version used by this project: Chromium 144

### Firefox 151

- Project: [LoseNine/firefox-fingerprintBrowser](https://github.com/LoseNine/firefox-fingerprintBrowser)
- Automation library: [LoseNine/ruyipage](https://github.com/LoseNine/ruyipage)
- Bundled version used by this project: Firefox 151

### Automation clients

- Recommended for Chrome: [Patchright](https://github.com/Kaliiiiiiiiii-Vinyzu/patchright)
- Recommended for Firefox: [RuyiPage](https://github.com/LoseNine/ruyipage)

## Features

- Chrome and Firefox fingerprint engines
- Dedicated user-data directory per profile
- Saved proxies, connection test, and bulk proxy assignment
- Global extension manager with per-profile extension overrides
- Local API Key, backend API mode, and readable API documentation
- Group based batch start and stop
- Single-instance desktop app with tray minimize support

## Fingerprint support

| Item | Chrome | Firefox |
| --- | --- | --- |
| Stable fingerprint identity | `fingerprint id` | Managed through `fpfile` |
| Language and timezone from IP | Supported | Supported |
| Platform settings | Windows / macOS / Linux | Font system selection |
| CPU threads | Auto / manual / random | Auto / manual / random |
| Screen settings | Launch window size | Auto / manual / random screen size |
| WebRTC | Follows proxy and launch settings | Auto / manual / random |
| WebGL | Provided by the engine | Auto / manual / random |
| Extra launch arguments | Supported | Supported |
| Startup URLs | Supported | Supported |
| Global extensions | Supported | Supported |

## Main Chrome options

- Stable or regenerated fingerprint id
- Auto language and timezone resolution from IP
- Manual language, Accept-Language, and timezone override
- Platform selection
- CPU thread mode: auto, manual, random
- Disable selected spoofing modules
- Extra launch arguments
- Launch window size
- Startup URLs
- Per-profile global extension override

## Main Firefox options

- Auto language and timezone resolution from IP
- Manual language and timezone override
- Font system selection
- Screen size mode: auto, manual, random
- WebGL mode: auto, manual, random
- CPU thread mode: auto, manual, random
- WebRTC mode: auto, manual, random
- Built-in WebRTC block extension
- Extra fingerprint fields
- Custom fingerprint file path
- Extra launch arguments
- Startup URLs
- Per-profile global extension override

## Usage

### 1 Create a browser profile

- Choose Chrome or Firefox
- Fill in name, group, and remark
- Select a saved proxy or use direct connection
- Adjust fingerprint settings for the selected engine
- Save and launch

### 2 Manage proxies

- Save commonly used proxies in the proxy manager
- Test connectivity before assigning them
- Assign one proxy to multiple profiles in bulk

### 3 Manage extensions

- Upload Chrome and Firefox extensions separately
- Enabled extensions load automatically for profiles using the same engine
- A single profile can disable selected global extensions

### 4 Use the local API

- Open API Access in the sidebar to view the local URL and API Key
- Open API Docs for endpoints, parameters, and examples
- The profile start endpoint returns the automation debug port as `port`

## Automation examples

### Chrome

After Chrome starts, use the returned local debug port with Patchright through CDP

### Firefox

After Firefox starts, use the returned port with RuyiPage

## Source code and build

- The source code is publicly available
- To reduce commercial abuse, this README does not include ready-to-run build steps
- If you want to study the build flow, please review the source tree and build scripts on your own

## Notes

- The Releases page provides ready-to-install builds
- The source repository intentionally excludes engine binaries, build outputs, runtime data, and local test cache
- Frontend source code is under `frontend/src`
- Backend API entry is `backend/main.py`

## Usage boundaries

- This is an open-source project and it is not sold as a paid product
- This project is intended for local development, automation debugging, testing, and compliant research
- Do not use this project for illegal activity, unauthorized access, platform abuse, or infringement
- Users are responsible for following local laws and the rules of any platform they interact with
