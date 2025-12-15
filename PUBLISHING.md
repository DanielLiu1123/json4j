# Publishing

## Prerequisites

- Go to [Maven Central](https://central.sonatype.com/) and create an account, then generate a user token

- Generate a GPG key pair

    ```shell
    gpg --gen-key
    gpg --list-keys
    gpg --armor --export <key-id> --output public.gpg
    gpg --armor --export-secret-key <key-id> --output secret.gpg
    gpg --keyserver keyserver.ubuntu.com --send-keys <key-id>
    ```

- Add secrets to GitHub repository
    
    ```shell
    gh secret set MAVENCENTRAL_USERNAME --body "your-username"
    gh secret set MAVENCENTRAL_PASSWORD --body "your-password"
    gh secret set GPG_SECRET_KEY --body < /path/to/secret.gpg
    gh secret set GPG_PASSPHRASE --body "your-gpg-passphrase"
    ```

## Publish Snapshot

```shell
export MAVENCENTRAL_USERNAME="your-username"
export MAVENCENTRAL_PASSWORD="your-password"
./gradlew clean publish -Pversion=1.0.0-SNAPSHOT
```

## Publish Release

```shell
# Step 1: Stage artifacts and sign
export GPG_SECRET_KEY="$(cat /path/to/secret.gpg)"
export GPG_PASSPHRASE="your-gpg-passphrase"
./gradlew clean publish -Pversion=1.0.0

# Step 2: Upload
export MAVENCENTRAL_USERNAME="your-username"
export MAVENCENTRAL_PASSWORD="your-password"
./gradlew deploy
```
