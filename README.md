# sftp-sync

将本地目录同步到 SFTP 服务器。工具默认建立 3 条独立 SSH/SFTP 连接并发处理文件。工具使用 SHA-256 计算文件内容，并将本地相对路径及哈希保存到 bbolt 数据库；首次运行会上传全部文件，之后仅上传新增或内容变化的文件。目录结构会保留，符号链接和非普通文件会跳过。

## 配置

复制 `sftp-sync.example.json` 为 `sftp-sync.json` 并按实际环境修改。认证支持 SSH 私钥或密码；服务器身份默认使用当前用户的 `~/.ssh/known_hosts` 校验，也可以通过 `host_key_file` 指定其他 OpenSSH `known_hosts` 文件。私钥认证优先支持，若同时设置密码会一并提供给 SSH 客户端。

```sh
go run . -config sftp-sync.json
```

也可以构建后运行：

```sh
go build -o sftp-sync .
./sftp-sync -config sftp-sync.json
```

`ignore` 项使用相对路径的 glob 模式；例如 `*.tmp` 忽略匹配的文件，`.git/**` 忽略 `.git` 目录及其内容。哈希数据库只在远端文件完整写入成功后更新。
