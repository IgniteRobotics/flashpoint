# 0019. Docs site: Zensical, versioned with the Zensical mike fork

- Status: Proposed
- Date: 2026-11-20
- Refs: change `mvp-documentation` (design D-a, D-c; task 1.1 has the spike results); issue #82

## Context
The MVP needs a published, versioned docs site built from `docs/guide/` (`https://igniterobotics.github.io/flashpoint/<version>/`), with a strict link check in CI. The usual Python choice, MkDocs + Material for MkDocs, is ending:

- Material for MkDocs is in maintenance mode. Its README states end of life on **2027-05-05**; until then it gets critical fixes and security updates only (9.8.2, 2026-11-19, is the latest).
- MkDocs 2.0 doesn't work with Material: TOML config, and a new plugin and template system.
- Material's team builds the successor, **Zensical** (MIT). It reads existing `mkdocs.yml` projects. It's young: 0.1.3 on 2026-11-20, "Development Status :: 4 - Beta", no public plugin API yet.
- Zensical has no versioning of its own yet. Its team keeps a fork of mike (based on mike 2.2.0, compatibility fixes only) as the stopgap. The fork isn't on PyPI.

Facts checked 2026-11-20 on PyPI, GitHub and Material's README.

## Decision
- The docs site is built with **Zensical**, pinned exactly (`zensical = "0.1.3"`) in an optional Poetry group `docs`.
- Versions are deployed to `gh-pages` with the **Zensical mike fork**, installed from `git+https://github.com/squidfunk/mike.git` pinned to a commit (`2d4ad799442f4592db8ad53b179bfb33db8c69ac`, 2026-04-17). Dependabot doesn't bump git dependencies, so a bump is a deliberate PR.
- Config stays in `mkdocs.yml`. **Feature-subset rule:** it uses only features that both Zensical and Material 9.8 support, so the fallback is a dependency swap, not a rewrite:
  - theme features `navigation.tabs`, `navigation.sections`, `navigation.top`, `search`, `content.code.copy`, `content.tabs.link`, and the palette toggle;
  - Markdown extensions `admonition`, `pymdownx.details`, `pymdownx.superfences`, `pymdownx.tabbed`, `attr_list`, `md_in_html`, `tables`, and `toc` with permalinks;
  - `extra.version.provider: mike`;
  - `INHERIT` overlays (`mkdocs.dev.yml` for the unreleased banner) and a `theme.custom_dir` override of the `announce` block. A template override is allowed; plugins are not.
- CI runs `zensical build --strict` as its own step before any deploy. mike's deploy builds without `--strict`, so the strict build is what fails a broken link or anchor.

**Fallback trigger.** Switch the `docs` group to `mkdocs==1.6.1`, `mkdocs-material==9.8.*` (latest patch, pinned) and `mike==2.*` from PyPI, with the same `mkdocs.yml` and `mkdocs build --strict`, if any of these stops being true on a Zensical or fork upgrade:
- the strict build fails on a broken link and on a broken anchor;
- the mike fork deploys two versions plus `latest` and `dev` to `gh-pages`;
- search works under a sub-path (`/flashpoint/<version>/`);
- content tabs work.

The 2026-11-20 spike checked all four on Zensical 0.1.3 and the fork at the commit above; none fired. If we ever fall back, moving back to Zensical before 2027-05-05 is a follow-up issue.

## Consequences
- One Python-native toolchain, the same family ignition-labs uses (on Material 9.7.7 as of 2026-11-14), with a successor path instead of an end-of-life one.
- We depend on a beta builder and an unreleased fork. The exact pins, the feature-subset rule and the fallback trigger keep that risk to a dependency swap.
- The docs group is optional: `poetry install` without `--with docs` doesn't fetch it, so the app's CI and users are unaffected.
- Today's guide doesn't pass the strict build: 24 links point to files outside `docs_dir` (`docs/rewrite/`, `docs/adr/`, `notebooks/`, `deploy/`, `config/`). They become `https://github.com/IgniteRobotics/flashpoint/blob/main/...` links before the strict build becomes a CI gate (`mvp-documentation` D-b, group 2).
- Zensical's strict failure ends with a Python traceback after the issue list. It's noise, not a second failure.
- Zensical's strict build doesn't check images or video: a missing `assets/media/` file builds clean (found 2026-11-20, after the spike). `tools/docs-media.py check-site` runs after the strict build and fails on any image or video a built page shows whose file isn't in the site, and the unit link check requires every `assets/media/` link to name a file the media manifest produces.

## Alternatives considered
- **MkDocs 1.6.1 + Material 9.8 + PyPI mike now:** stable and known, but fixes end 2027-05-05, four months after the announce. It's the fallback, not the plan.
- **Sphinx + Furo + MyST:** maintained, but every page would need MyST idioms and toctree navigation, versioning tools are stale (sphinx-multiversion), and it's a second toolchain next to ignition-labs.
- **MkDocs 2.0:** no Material theme, and its plugin ecosystem is in flux.
- **Docusaurus or Starlight:** Node-based; the repo standard is Python-native.
