# CI version pins

Some versions used by CI are pinned manually and must be kept current. This
file is the checklist of what needs periodic review and where each pin lives.

| What | Version | Defined in | Notes |
|------|---------|------------|-------|
| Java (JDK) | `17` | `.github/actions/setup-java/action.yml` | Single source of truth for the CI JDK. Must match `maven.compiler.source`/`maven.compiler.target` in `pom.xml`. Review when the project adopts a new LTS. |

## Automatically maintained

GitHub Action versions (the `uses:` refs in `.github/workflows/*` and
`.github/actions/**`) are pinned to commit SHAs and bumped by Dependabot — see
`.github/dependabot.yml`. These do **not** need manual tracking here.
