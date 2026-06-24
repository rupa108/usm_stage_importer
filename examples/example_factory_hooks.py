# -*- coding: utf-8 -*-
"""Demonstrates the factory customization hooks.

A factory is the intended place to *mangle the data* coming from the staging
repository and to *customize how processors are instantiated*. Rather than
re-implementing the whole `process_all` loop, you subclass the factory and
override one or more of the provided hooks:

    Hook A - build_processor(tr, processor_class, source, target, **kwargs)
        The single place where processors are constructed. Override to pass
        extra constructor args, inject shared services/caches, or wrap the
        processor instance.

    Hook B - prepare_source_record(tr, row_bo)
        Mangle/enrich the raw staging record before mapping. Return None (or
        `undefined`) to skip the record entirely.

    Hook C - lifecycle/cross-cutting hooks:
        should_process(tr, source_record)      -> per-record filter
        on_target_created(tr, target, source)  -> notification on create
        on_target_found(tr, target, source)    -> notification on update
        on_record_failed(tr, row_bo, exc)      -> custom error handling

All hooks have no-op/identity base implementations, so overriding is optional
and fully backward compatible.
"""

import traceback

from stage_importer_framework import (
    ImportOrchestrator,
    MappingProcessorFactory,
    StagingRepository,
    MappingProcessor,
    PlainField,
    log_,
)


# ==============================================================================
# 1. A SHARED SERVICE WE WANT TO INJECT INTO EVERY PROCESSOR
# ==============================================================================

class SiteResolver(object):
    """A trivial example of a service that we want to share across processors.

    In a real import this might wrap an expensive lookup table or a remote
    call that should be performed once and reused for every record.
    """
    def __init__(self):
        self._cache = {"BER": "Berlin", "NYC": "New York City"}

    def resolve(self, site_code):
        return self._cache.get(site_code, "Unknown Site")


# ==============================================================================
# 2. PROCESSOR THAT EXPECTS THE INJECTED SERVICE
# ==============================================================================

class SystemProcessor(MappingProcessor):
    """Maps a system and uses the injected `site_resolver` in post_process."""
    __processing_order__ = ('systemname', 'serialnumber')

    systemname = PlainField(source_field="NAME")
    serialnumber = PlainField(source_field="SERIAL")

    def post_process(self):
        # `self.site_resolver` is injected by the factory's build_processor hook.
        site_code = self.source.getBOField("SITE_CODE").getValue()
        site_name = self.site_resolver.resolve(site_code)
        self.target.getBOField("location").setValue(site_name)


# ==============================================================================
# 3. CUSTOM FACTORY OVERRIDING THE HOOKS
# ==============================================================================

class HookedSystemFactory(MappingProcessorFactory):
    """A factory demonstrating all of the customization hooks."""

    def __init__(self, *args, **kwargs):
        super(HookedSystemFactory, self).__init__(*args, **kwargs)
        # Build expensive/shared state once and reuse it for every record.
        self.site_resolver = SiteResolver()
        self.skipped_count = 0

    # --- Hook B: mangle/enrich the raw staging record -------------------------
    def prepare_source_record(self, tr, row_bo):
        """Normalize key fields before they are used for matching/mapping."""
        # Normalize the serial number: trim whitespace and upper-case it.
        serial_field = row_bo.getBOField("SERIAL")
        serial = serial_field.getValue()
        if serial:
            serial_field.setValue(serial.strip().upper())
        return row_bo

    # --- Hook C: per-record filter --------------------------------------------
    def should_process(self, tr, source_record):
        """Skip records that are flagged as obsolete in staging."""
        if source_record.getBOField("STATUS").getValue() == "OBSOLETE":
            self.skipped_count += 1
            return False
        return True

    # --- Hook A: customize processor instantiation ----------------------------
    def build_processor(self, tr, processor_class, source_bo, target_bo, **kwargs):
        """Inject the shared SiteResolver into every processor instance."""
        processor = super(HookedSystemFactory, self).build_processor(
            tr, processor_class, source_bo, target_bo, **kwargs)
        processor.site_resolver = self.site_resolver
        return processor

    # --- Hook C: creation/update notifications --------------------------------
    def on_target_created(self, tr, target_bo, source_record):
        log_("Created new System '%s'." % target_bo.getMoniker(), VM.LOG_INFO, target_bo)

    def on_target_found(self, tr, target_bo, source_record):
        log_("Updating existing System '%s'." % target_bo.getMoniker(), VM.LOG_DEBUG, target_bo)

    # --- Hook C: custom error handling ----------------------------------------
    def on_record_failed(self, tr, row_bo, exc):
        log_("Record '%s' failed and will be retried next run." % row_bo.getMoniker(), VM.LOG_WARN, row_bo)

    def get_summary(self):
        base = super(HookedSystemFactory, self).get_summary()
        return "%s\nSkipped (filtered): %d" % (base, self.skipped_count)


# ==============================================================================
# 4. MAIN EXECUTION BLOCK
# ==============================================================================

if __name__ == "__main__":
    try:
        factory = HookedSystemFactory(
            repository=StagingRepository("StagingSystems"),
            default_processor_class=SystemProcessor,
            target_bo_name="System",
            source_key="SERIAL",
            target_key="serialNumber",
        )

        orchestrator = ImportOrchestrator(factories=[factory])
        orchestrator.run()

    except Exception as e:
        log_("A critical error occurred during the factory hooks example: %s" % e, VM.LOG_ERROR)
        traceback.print_exc()
