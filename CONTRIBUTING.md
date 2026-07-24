# Contributing

To build

```
mvn clean install
```

To test

```
mvn test
```

## Continuous integration

Version pins and the secrets the publishing jobs need are documented in
[`.github/CI_VERSIONS.md`](.github/CI_VERSIONS.md).

## Releasing

Releases are tag-driven. Pushing a `vX.Y.Z` tag runs `release.yml`, which
publishes to Maven Central and to GitHub Packages and creates a GitHub Release
with the jars attached. Nothing is published by hand, and there is no
`maven-release-plugin` — the tag is both the trigger and the source of the
version.

### Cutting a release

1. Make sure `master` is green and contains everything the release should ship.
2. Tag that commit and push the tag:

   ```
   git tag -a v0.2.3 -m "v0.2.3"
   git push origin v0.2.3
   ```

3. Watch the `Release` run. It will, in order:
   - run the tests;
   - set the pom version from the tag (`versions:set`, leading `v` stripped);
   - publish GPG-signed artifacts to Maven Central via the `releases` profile;
   - publish to GitHub Packages;
   - create the GitHub Release.
4. Bump `<version>` in `pom.xml` to the next `-SNAPSHOT` and commit that to
   `master`.

Step 4 is not optional: the release run sets the version only in the runner's
checkout and never commits it, so `master` carries a `-SNAPSHOT` version at all
times and only step 4 moves it forward.

Tags containing `-rc`, `-beta` or `-alpha` (for example `v0.3.0-rc1`) are
published as GitHub *pre-releases*; anything else is a full release.

### Snapshots

Every push to `master` deploys a `-SNAPSHOT` to the Central snapshot repository
(`merge.yml` → `_deploy.yml`). Snapshot versions are mutable, so no tag and no
signing are involved.

## Tag protection

To keep an accidental or mistyped tag from publishing a release, `refs/tags/v*`
is covered by two GitHub rulesets (**Settings → Rules → Rulesets**). Two, not
one, because a ruleset's bypass list exempts an actor from all of its rules:
admins need to be able to clean up a broken tag, but must not be able to skip
the naming check.

| Ruleset | Rules | Bypass | Effect |
|---------|-------|--------|--------|
| Release tag names | `tag_name_pattern` | nobody | The name must be `vMAJOR.MINOR.PATCH`, optionally with an `-rc`/`-beta`/`-alpha` suffix — `v0.2.3` and `v0.3.0-rc1` are accepted, `v0.2` and `v0.2.3.1` are not. |
| Release tags are admin-only and immutable | `creation`, `update`, `deletion`, `non_fast_forward` | repository admins | Only admins can create a release tag, and once pushed it cannot be moved or deleted. |

This matters beyond tidiness: a published version is spent. Maven Central and
GitHub Packages both refuse to re-publish an existing version, so a release run
that fails partway cannot be repaired by deleting and re-pushing the tag — fix
the cause and cut the next patch version.
