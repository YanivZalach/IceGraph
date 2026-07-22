# IceGraph Architecture Philosophy

Four principles guiding IceGraph's design. Check new features, dependencies, and architectural changes against them.

## 1. Zero footprint on Spark/Iceberg

IceGraph is strictly read-only against the tables it inspects: no writes, no mutations, no commits. Enforce this at the credential level too - the Spark Connect identity used in production should itself be read-only, so the guarantee holds even if application logic has a bug.

## 2. Minimal moving parts

Introduce no service or infrastructure component unless the feature is impossible without it. Each addition is operational surface the user must run, monitor, and keep available.

## 3. Simple deployment

`SPARK_REMOTE` is the only mandatory configuration. All other tunables default in `backend/constants.py`, overridable via environment variable. One Docker image serves both frontend and backend; no second service to provision or wire up.

## 4. The process is disposable

In-memory job state is a cache of a computation, not a record of one - process termination loses no data, since the source of truth is Iceberg's metadata, untouched. Recovery requires zero operator action: the next client request reconstructs the same result.
