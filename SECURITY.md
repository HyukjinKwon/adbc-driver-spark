# Security Policy

## Supported versions

This project is pre-1.0. Security fixes are applied to the latest released
version on the `main` branch. Please upgrade to the most recent release before
reporting an issue.

## Reporting a vulnerability

Please do not open a public issue for security vulnerabilities.

Use GitHub's private vulnerability reporting for this repository:

1. Go to the **Security** tab of the repository.
2. Click **Report a vulnerability**.
3. Provide a description, reproduction steps, and the affected version.

You can expect an acknowledgement within a few business days. Once a fix is
available we will coordinate a release and credit the reporter unless anonymity
is requested.

## Scope

This driver connects to a Spark Connect server that you control or trust. Treat
the connection string and any bearer tokens as secrets: they grant access to your
Spark cluster. Always use `use_ssl=true` for connections that leave localhost.
