# Keycloak Role Event SPI

A production-grade Keycloak Service Provider Interface (SPI) designed to asynchronously capture role assignment and removal events (for both realm and client roles) and publish them to an Apache Kafka topic.

## Features
- **Asynchronous Publishing**: Uses Kafka's non-blocking `send()` mechanism.
- **Singleton Kafka Producer**: Reuses a single Kafka producer instance across the Keycloak lifecycle.
- **Production Grade configuration**: Includes idempotent configuration and retry-safe logic based on Kafka best practices.
- **Environment Aware**: Dynamically loads `application-{env}.properties` based on the `APP_ENV` environment variable, enabling seamless transitions between dev, qa, and prod.
- **Dynamic Credential Resolution**: Parses `SASL_SSL` and `SCRAM-SHA-512` credentials dynamically using environment variables like `${KAFKA_USERNAME}`.

## Prerequisites
- **Java**: 17
- **Keycloak**: 24.0+ (Tested against recent Keycloak versions using latest SPI)
- **Apache Kafka**: 4.1.1 
- **Maven**: 3.8+

## Building the Plugin
Run the following Maven command to build the project:

```bash
mvn clean package
```
This generates a shaded JAR in the `target/` directory (e.g., `role-event-spi-1.0.0-SNAPSHOT-shaded.jar`), which bundles the required `kafka-clients` and `jackson` libraries so Keycloak doesn't encounter `ClassNotFoundException`.

## Deployment
1. Copy the resulting shaded JAR to your Keycloak `providers/` directory:
   ```bash
   cp target/role-event-spi-1.0.0-SNAPSHOT-shaded.jar /opt/keycloak/providers/
   ```
2. Rebuild the Keycloak optimized image (if running in production mode):
   ```bash
   /opt/keycloak/bin/kc.sh build
   ```
3. Start Keycloak:
   ```bash
   /opt/keycloak/bin/kc.sh start
   ```

## Configuration

Set the `APP_ENV` environment variable to determine which properties file is loaded (`dev`, `qa`, or `prod`). 
Default is `dev` if not set.

Ensure you provide the corresponding Kafka credentials to the Keycloak container/environment:
```bash
export APP_ENV=prod
export KAFKA_USERNAME=your_username
export KAFKA_PASSWORD=your_password
```

### Property Files
The SPI relies on properties located in `src/main/resources/`. Edit the specific `application-{env}.properties` files to change your `bootstrap.servers`, topic name, or SSL configurations.

## Enabling the SPI in Keycloak
1. Log in to the Keycloak Admin Console.
2. Go to **Realm Settings** > **Events**.
3. Under the **Config** tab, add `role-event-kafka-publisher` to the **Event Listeners** list.
4. Save the configuration.

Now, whenever an administrator assigns or removes a role, a JSON message will be published to the configured Kafka topic.
