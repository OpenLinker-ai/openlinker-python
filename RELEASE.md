# Release Process

Chinese documentation: [RELEASE.zh-CN.md](./RELEASE.zh-CN.md)

`openlinker-python` is not published on PyPI yet. Publishing a distribution,
creating a GitHub release, or pushing a tag is an explicit maintainer action and
is not implied by passing local checks.

## Pre-Release Checklist

1. Choose the release version and make `pyproject.toml`, `CHANGELOG.md`, the SDK
   agent string, tag, wheel, and release notes agree.
2. Confirm README, PARITY, CONTRIBUTING, SECURITY, SUPPORT, contracts, and
   examples match the release source.
3. Run:

   ```bash
   python -m pip install -e ".[dev,grpc]"
   python -m ruff check .
   python -m compileall -q src
   python -m pytest
   python -m build
   ```

4. Inspect wheel and source archive contents and metadata.
5. Run a secret scan on a clean checkout and confirm no local state or credentials
   are packaged.
6. Validate the Runtime contract and optional gRPC path against the intended Core
   release environment.

## Publishing

Publish to the configured PyPI account only after the maintainer has approved the
version and release artifacts. Then create a matching signed semantic-version tag
and GitHub release. Pre-1.0 breaking changes must be explicit in CHANGELOG and the
release notes.
