@echo off
chcp 65001 >nul
echo [SyncT] 1/3 正在生成 bailian-color.png 应用图标与 asInvoker 权限清单资源 (.syso)...
set CGO_ENABLED=0
go test -run TestGenerateWindowsSysoAndICO ./internal/winres
if %ERRORLEVEL% neq 0 (
    echo [错误] 生成图标与清单资源失败！
    exit /b %ERRORLEVEL%
)

echo [SyncT] 2/3 正在构建 Windows 11 原生便携版 SyncT.exe (CGO_ENABLED=0)...
set GOOS=windows
set GOARCH=amd64
go build -trimpath -ldflags="-H windowsgui -s -w" -o SyncT.exe .
if %ERRORLEVEL% neq 0 (
    echo [错误] 编译失败！
    exit /b %ERRORLEVEL%
)

echo [SyncT] 3/3 正在打包 Release ZIP 发布包 (本机服务端 + 工作电脑 User 低权限客户端)...
go test -run TestPackagePortableReleaseZIPs ./internal/winres
if %ERRORLEVEL% neq 0 (
    echo [错误] 打包 Release ZIP 失败！
    exit /b %ERRORLEVEL%
)

echo [SyncT] 构建与打包完成！请查看 release\ 目录下的 ZIP 发布包。
