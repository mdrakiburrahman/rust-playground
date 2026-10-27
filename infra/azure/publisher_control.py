"""Cooperative publisher authority v1. No public reset execution is enabled."""

import json
import uuid

from bootstrap import Failure, publisher_registry, require_released

READY = "ready-v1"
FENCED = "fenced-v1"
MAX_METADATA_BYTES = 8192


class UnknownTransition(Failure):
    """A submitted metadata mutation has no proven response; never retry blindly."""


def canonical_uuid(value):
    try:
        if not isinstance(value, str) or str(uuid.UUID(value)) != value:
            raise ValueError("noncanonical UUID")
    except (ValueError, TypeError, AttributeError) as exc:
        raise Failure("Publisher authority requires canonical UUIDs.") from exc
    return value


def encode_registry(metadata, registry):
    updated = dict(metadata)
    updated["publishers"] = json.dumps(registry, separators=(",", ":"), sort_keys=True)
    publisher_registry(updated)
    # Include metadata header names, separators and CRLF, not just value bytes.
    size = sum(len(("x-ms-meta-" + key + ": " + value + "\r\n").encode("utf-8"))
               for key, value in updated.items())
    if size > MAX_METADATA_BYTES:
        raise Failure("Container metadata budget exhausted; admission fails closed.")
    return updated


class PublisherControl:
    """Same-container ETag protocol; every operation performs at most one CAS."""

    def __init__(self, azure):
        self.azure = azure

    def read(self):
        _, headers = self.azure.blob(
            "HEAD", self.azure.c.checkpoints, query={"restype": "container"},
        )
        metadata = {key[10:]: value for key, value in headers.items()
                    if key.startswith("x-ms-meta-")}
        if metadata.get("sandboxid") != self.azure.c.sandbox:
            raise Failure("Publisher authority sandbox ownership mismatch.")
        if metadata.get("resetstate") not in (READY, FENCED):
            raise Failure("Publisher authority requires versioned ready-v1/fenced-v1 state.")
        canonical_uuid(metadata.get("resetgeneration"))
        registry = publisher_registry(metadata)
        if any(entry["generation"] != metadata["resetgeneration"]
               for entry in registry.values()):
            raise Failure("Publisher registry contains a different authority generation.")
        if not headers.get("etag"):
            raise Failure("Publisher authority ETag is missing.")
        encode_registry(metadata, registry)
        return metadata, registry, headers["etag"]

    def cas(self, metadata, registry, etag):
        metadata = encode_registry(metadata, registry)
        try:
            self.azure.set_control(metadata, etag)
        except Failure as exc:
            # Includes 412 conflicts: no success receipt and no automatic replay.
            # A network/timeout failure may have applied the mutation remotely.
            raise UnknownTransition(
                "Publisher metadata CAS did not produce a definitive success; "
                "do not admit mutation or infer drain. Preserve publisher identity "
                "and reconcile authority read-only."
            ) from exc
        return metadata

    def admit(self, publisher, generation, bootstrap=False):
        publisher, generation = canonical_uuid(publisher), canonical_uuid(generation)
        metadata, registry, etag = self.read()
        if metadata["resetgeneration"] != generation:
            raise Failure("Admission generation changed.")
        expected = FENCED if bootstrap else READY
        if metadata["resetstate"] != expected:
            raise Failure("Publisher admission is fenced or has incompatible authority.")
        if bootstrap:
            require_released(metadata)
            if registry:
                raise Failure("Bootstrap requires exclusive empty publisher registry.")
        if publisher in registry:
            raise Failure("Publisher identity is already registered; no implicit retry/adoption.")
        registry[publisher] = {"generation": generation, "state": "active"}
        return self.cas(metadata, registry, etag)

    def fence(self, generation):
        generation = canonical_uuid(generation)
        metadata, registry, etag = self.read()
        require_released(metadata)
        if metadata["resetgeneration"] != generation:
            raise Failure("Cannot fence a different generation.")
        metadata["resetstate"] = FENCED
        # Preserve member generation: admitted publishers may still clean-drain.
        return self.cas(metadata, registry, etag)

    def member(self, publisher, generation):
        publisher, generation = canonical_uuid(publisher), canonical_uuid(generation)
        metadata, registry, etag = self.read()
        if metadata["resetgeneration"] != generation or publisher not in registry:
            raise Failure("Publisher registration or generation no longer matches.")
        return publisher, metadata, registry, etag

    def uncertain(self, publisher, generation):
        publisher, metadata, registry, etag = self.member(publisher, generation)
        registry[publisher]["state"] = "uncertain"
        return self.cas(metadata, registry, etag)

    def clean_release(self, publisher, generation, *, definitively_drained):
        publisher, metadata, registry, etag = self.member(publisher, generation)
        if definitively_drained is not True:
            raise Failure("Clean release requires definitive completion of every physical request.")
        if registry[publisher]["state"] != "active":
            raise Failure("An uncertain publisher cannot be force-released or expire.")
        del registry[publisher]
        return self.cas(metadata, registry, etag)

    def drained(self, generation):
        metadata, registry, etag = self.read()
        if metadata["resetstate"] != FENCED or metadata["resetgeneration"] != generation:
            raise Failure("Drain proof requires the same fenced authority generation.")
        if registry:
            raise Failure("Active or uncertain registered publishers block cleanup.")
        require_released(metadata)
        return metadata, etag

    def rotate_drained(self, generation, new_generation):
        canonical_uuid(new_generation)
        metadata, etag = self.drained(generation)
        if new_generation == generation:
            raise Failure("Generation rotation requires a fresh UUID.")
        metadata["resetgeneration"] = new_generation
        return self.cas(metadata, {}, etag)

    def complete_bootstrap(self, publisher, generation, *, definitively_drained):
        publisher, metadata, registry, etag = self.member(publisher, generation)
        require_released(metadata)
        if (metadata["resetstate"] != FENCED or set(registry) != {publisher} or
                registry[publisher]["state"] != "active" or definitively_drained is not True):
            raise Failure("Bootstrap readiness requires its exclusive definitively drained operator.")
        metadata["resetstate"] = READY
        # The operator releases and enables admission in one ETag transition.
        return self.cas(metadata, {}, etag)
