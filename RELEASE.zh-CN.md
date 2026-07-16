# 发布流程

English documentation: [RELEASE.md](./RELEASE.md)

`openlinker-python` 尚未发布到 PyPI。发布 distribution、创建 GitHub Release 或推送 tag
都需要维护者明确执行，不能因为本地检查通过就视为已经获得发布授权。

## 发布前检查

1. 确定版本，让 `pyproject.toml`、`CHANGELOG.md`、SDK agent 字符串、tag、wheel 和
   Release Notes 完全一致。
2. 确认 README、PARITY、CONTRIBUTING、SECURITY、SUPPORT、契约和示例与发布源码一致。
3. 运行：

   ```bash
   python -m pip install -e ".[dev,grpc]"
   python -m ruff check .
   python -m compileall -q src
   python -m pytest
   python -m build
   ```

4. 检查 wheel、源码包内容和 metadata。
5. 在干净 checkout 上扫描 secret，并确认本地状态和凭据没有进入包。
6. 在目标 Core release 环境验证 Runtime 契约和可选 gRPC 路径。

## 发布

只有维护者确认版本和产物后，才能发布到指定 PyPI 账号。随后创建完全匹配、带签名的
语义版本 tag 和 GitHub Release。pre-1.0 breaking change 必须写入 CHANGELOG 和
Release Notes。
