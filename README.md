# template-gradle

A Gradle project template with integrated GitHub actions, linting, formatting, and Maven Central publishing workflow.

```shell
gh repo create <your-repo-name> --description <your-repo-description> --template DanielLiu1123/template-gradle --public
```

## Table of Contents

- [Build](#build)
- [Publishing](#publishing)

## Build

This project uses [Spotless](https://github.com/diffplug/spotless) and [Spotbugs](https://github.com/spotbugs/spotbugs) for formating and linting.

```shell
./gradlew build
```

## Publishing

See [PUBLISHING.md](./PUBLISHING.md).
