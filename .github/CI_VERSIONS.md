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

## Required repository secrets

Publishing (`merge.yml` snapshot deploy and `release.yml`) relies on these
secrets being configured under **Settings → Secrets and variables → Actions**:

| Secret | Used for |
|--------|----------|
| `SONATYPE_USERNAME` | Maven Central / Sonatype user token (server id `central`). |
| `SONATYPE_PASSWORD` | Maven Central / Sonatype token password. |
| `GPG_PRIVATE_KEY` | Base64-encoded GPG private key used to sign Central artifacts. |
| `GPG_PASSPHRASE` | Passphrase for the GPG key (`MAVEN_GPG_PASSPHRASE`). |

`GITHUB_TOKEN` is provided automatically and is used to publish to GitHub
Packages and to create the GitHub Release.

## Releasing

Push a tag of the form `vX.Y.Z` (e.g. `v0.2.3`). The `Release` workflow derives
the version from the tag, publishes to Maven Central and GitHub Packages, and
creates a GitHub Release with the jars attached. Tags containing `-rc`, `-beta`,
or `-alpha` are published as pre-releases.
