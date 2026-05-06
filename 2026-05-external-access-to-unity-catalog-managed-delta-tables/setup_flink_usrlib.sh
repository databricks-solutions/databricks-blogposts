#!/usr/bin/env bash
#
# setup_flink_usrlib.sh
#
# Pulls the Delta Flink Connector + all transitive dependencies from Maven
# Central into a target Flink usrlib/ directory, ready for the local
# docker-compose Flink cluster (delta-io/delta `flink/docker/<ver>/usrlib/`).
#
# Why Maven and not the source build?
#   The Delta Flink Connector ships on Maven Central as a *thin* JAR (just
#   the connector classes, ~150 KB). Its transitive deps — Delta Kernel
#   defaults / UC integration, Hadoop AWS, AWS SDK bundle, Parquet, etc. —
#   are NOT bundled. So we use Maven to resolve the dep tree and copy the
#   JARs into usrlib. That avoids the ~15-minute `sbt flink/assembly` build
#   plus the UC-OSS bootstrap dance the source path requires.
#
# Usage:
#   ./setup_flink_usrlib.sh /path/to/flink/docker/2.0/usrlib
#
# Prereqs:
#   * Java 17+
#   * Maven (`brew install maven`)
#   * Network access to Maven Central (or a mirror — see settings note below)
#
# Network mirror note:
#   On machines where /etc/hosts blocks `repo1.maven.org`, point Maven at
#   the GCS mirror by adding this to ~/.m2/settings.xml:
#
#     <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0">
#       <mirrors>
#         <mirror>
#           <id>gcs-mirror</id>
#           <url>https://maven-central.storage.googleapis.com/maven2</url>
#           <mirrorOf>central,*,!sbt-plugin-releases,!typesafe-ivy-releases</mirrorOf>
#         </mirror>
#       </mirrors>
#     </settings>
#
set -euo pipefail

DELTA_FLINK_VERSION="${DELTA_FLINK_VERSION:-4.2.0}"
USRLIB="${1:-}"

if [[ -z "$USRLIB" ]]; then
  echo "Usage: $0 <FLINK_DOCKER_USRLIB_PATH>" >&2
  echo "  e.g.  $0 ~/delta/flink/docker/2.0/usrlib" >&2
  exit 2
fi

if ! command -v mvn >/dev/null 2>&1; then
  echo "ERROR: 'mvn' not on PATH. Install Maven first (brew install maven)." >&2
  exit 1
fi

mkdir -p "$USRLIB"

# Build a tiny pom.xml that depends on delta-flink:$DELTA_FLINK_VERSION, then
# use mvn dependency:copy-dependencies to fetch the runtime closure into a
# scratch dir, then copy each JAR into usrlib.
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cat > "$WORK/pom.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>local.fetcher</groupId>
  <artifactId>delta-flink-deps</artifactId>
  <version>1.0</version>
  <packaging>jar</packaging>
  <dependencies>
    <dependency>
      <groupId>io.delta</groupId>
      <artifactId>delta-flink</artifactId>
      <version>$DELTA_FLINK_VERSION</version>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-dependency-plugin</artifactId>
        <version>3.6.1</version>
      </plugin>
    </plugins>
  </build>
</project>
EOF

echo ">>> Resolving io.delta:delta-flink:$DELTA_FLINK_VERSION + transitive runtime deps..."
( cd "$WORK" && mvn -q dependency:copy-dependencies \
    -DoutputDirectory=lib \
    -DincludeScope=runtime )

echo ">>> Copying JARs to $USRLIB"
cp "$WORK/lib/"*.jar "$USRLIB/"

# Refresh the connector summary so a re-run shows the latest staged JARs.
echo ""
echo ">>> Staged JARs (delta + bundle + a few highlights):"
ls -1 "$USRLIB/" | grep -E '^(delta-|bundle-|hadoop-aws|delta-kernel-)' | sed 's/^/    /'

echo ""
echo "Done. Next: from the Flink docker dir,"
echo "    cd $(dirname "$USRLIB")"
echo "    docker compose up -d"
