# Security policy

## Supported versions

Fixes go into the **latest minor release** only (for example, while 0.3.x is current, fixes ship as a new 0.3.x). Upgrade to the newest version on [PyPI](https://pypi.org/p/flashpoint) before reporting.

## Reporting a vulnerability

**Don't open a public issue for a security problem.** Report it privately through GitHub's form: [report a vulnerability](https://github.com/IgniteRobotics/flashpoint/security/advisories/new) (the repository's Security tab, then "Report a vulnerability"). Only the project's maintainers can see the report.

Please include the Flashpoint version, your OS, what an attacker could do, and the steps to reproduce it. Don't include match logs in the report; if we need one, we'll ask for it privately.

We aim to reply within a week, also during build season. Once a fix is released, we publish an advisory crediting you, unless you'd rather not be named.

## Scope

Flashpoint runs on a team's own laptop and reads that team's logs. Things we especially want to hear about: the local web server (`flashpoint serve`) being reachable or writable from another machine when it shouldn't be, a crafted `.wpilog` or `.hoot` that makes ingest run code or write outside the lake, and anything in the robot connection (`acquire`) that could change files on the robot.
