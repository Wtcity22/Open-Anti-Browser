#define MyAppName "Open-Anti-Browser"
#define MyAppVersion "0.1.2"
#define MyAppPublisher "Open-Anti-Browser"
#define MyAppExeName "Open-Anti-Browser.exe"
#define MyAppAssocName MyAppName + " Desktop App"

[Setup]
AppId={{9B5E79B8-1F20-4B82-9F7A-8F217DE8FD8B}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={localappdata}\Programs\{#MyAppName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
DisableProgramGroupPage=yes
OutputDir=..\dist\installer
OutputBaseFilename=Open-Anti-Browser-Setup
SetupIconFile=..\assets\app.ico
UninstallDisplayIcon={app}\{#MyAppExeName}
UninstallFilesDir={localappdata}\Open-Anti-Browser\uninstall
WizardResizable=yes

[Languages]
Name: "chinesesimp"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "创建桌面快捷方式"; GroupDescription: "附加任务:"; Flags: unchecked

[Files]
Source: "..\dist\Open-Anti-Browser\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\dist\Open-Anti-Browser\_internal\*"; DestDir: "{app}\_internal"; Flags: ignoreversion recursesubdirs createallsubdirs

[Dirs]
Name: "{app}\_internal"; Attribs: hidden system
Name: "{localappdata}\Open-Anti-Browser\uninstall"; Attribs: hidden system

[Icons]
Name: "{autoprograms}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{cmd}"; Parameters: "/C attrib +h +s ""{app}\_internal"" /S /D"; Flags: runhidden waituntilterminated
Filename: "{app}\{#MyAppExeName}"; Description: "启动 {#MyAppName}"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{localappdata}\Open-Anti-Browser\data\qt-webview"
Type: filesandordirs; Name: "{localappdata}\Open-Anti-Browser\data\qt-webview-cache"
