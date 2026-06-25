# -*- coding: utf-8 -*-
import unittest

from tests.lib import test_support

# Stub out the USM imports and seed the global VM BEFORE importing the framework.
VM = test_support.bootstrap()

# Now it is safe to import the framework module itself.
import stage_importer_framework as framework  # noqa: E402
from stage_importer_framework import (  # noqa: E402
    ImportOrchestrator,
    MappingProcessorFactory,
    StagingRepository,
    ReconciliationBaseline,
    Reconciler,
    MappingProcessor,
    PlainField,
    StaticField,
)


# ==============================================================================
# Example processor used across the tests.
# ==============================================================================

class SystemProcessor(MappingProcessor):
    """Maps a staging row into a ``System`` business object."""

    class Meta:
        target_bo_name = "System"

    __processing_order__ = ("systemname", "name", "description", "status")

    systemname = PlainField(source_field="CI_NR", match_key=True)
    name = PlainField(source_field="NAME")
    description = PlainField(source_field="DESCRIPTION", processor_func=lambda ctx, v: (v or "").strip())
    status = StaticField(value="ACTIVE")


# ==============================================================================
# Test case
# ==============================================================================

class StageImporterTestCase(unittest.TestCase):

    def setUp(self):
        # Reuse the single import-time VM (processor classes cached their
        # `target_type` against it) but clear its stores for test isolation and
        # get a fresh transaction with a zeroed commit counter.
        self.vm = VM
        self.tr = test_support.reset_environment(self.vm)

    def _make_staging_row(self, ci_nr, name, description):
        """Create a staging BO directly in the StagingSystem store."""
        row = self.vm.getBOType("StagingSystem").createBO(self.tr)
        row.getBOField("CI_NR").setValue(ci_nr)
        row.getBOField("NAME").setValue(name)
        row.getBOField("DESCRIPTION").setValue(description)
        return row

    def _build_orchestrator(self, with_reconciler=False):
        factory = MappingProcessorFactory(
            repository=StagingRepository("StagingSystem"),
            default_processor_class=SystemProcessor,
        )
        reconcilers = []
        if with_reconciler:
            baseline = ReconciliationBaseline(
                target_bo_name="System",
                active_condition="status == 'ACTIVE'",
            )
            reconcilers.append(Reconciler(reconciliation_baseline=baseline))
        return ImportOrchestrator(factories=[factory], reconcilers=reconcilers, tr=self.tr)

    # --------------------------------------------------------------------------
    # Tests
    # --------------------------------------------------------------------------

    def test_creates_new_target_objects(self):
        self._make_staging_row("CI-1", "Server A", "  Primary server  ")
        self._make_staging_row("CI-2", "Server B", "Backup server")

        self._build_orchestrator().run()

        systems = self.vm.get_store("System")
        self.assertEqual(len(systems), 2)

        by_key = dict((s.getBOField("systemname").getValue(), s) for s in systems)
        self.assertIn("CI-1", by_key)
        self.assertIn("CI-2", by_key)

        server_a = by_key["CI-1"]
        self.assertEqual(server_a.getBOField("name").getValue(), "Server A")
        # processor_func should have stripped surrounding whitespace.
        self.assertEqual(server_a.getBOField("description").getValue(), "Primary server")
        # StaticField should have set a constant value.
        self.assertEqual(server_a.getBOField("status").getValue(), "ACTIVE")

    def test_updates_existing_target_object(self):
        # Pre-existing target matching the staging row's key.
        existing = self.vm.getBOType("System").createBO(self.tr)
        existing.getBOField("systemname").setValue("CI-1")
        existing.getBOField("name").setValue("Old name")

        self._make_staging_row("CI-1", "New name", "desc")

        self._build_orchestrator().run()

        systems = self.vm.get_store("System")
        # No new object created; the existing one is reused.
        self.assertEqual(len(systems), 1)
        self.assertEqual(systems[0].getMoniker(), existing.getMoniker())
        self.assertEqual(systems[0].getBOField("name").getValue(), "New name")

    def test_reconciler_deactivates_obsolete_records(self):
        # An obsolete active record that is NOT represented in the staging data.
        obsolete = self.vm.getBOType("System").createBO(self.tr)
        obsolete.getBOField("systemname").setValue("OLD-1")
        obsolete.getBOField("status").setValue("ACTIVE")

        # Staging only knows about CI-1.
        self._make_staging_row("CI-1", "Server A", "desc")

        self._build_orchestrator(with_reconciler=True).run()

        # The imported record is created and the obsolete one is swept.
        keys = dict((s.getBOField("systemname").getValue(), s) for s in self.vm.get_store("System"))
        self.assertIn("CI-1", keys)
        # The reconciler deactivates obsolete records by stamping `validto`.
        self.assertTrue("validto" in obsolete)

    def test_failed_record_is_counted_and_does_not_abort_run(self):
        # A processor whose mapping raises for a specific row.
        class FlakyProcessor(MappingProcessor):
            class Meta:
                target_bo_name = "System"

            __processing_order__ = ("systemname", "name")

            systemname = PlainField(source_field="CI_NR", match_key=True)

            def _boom(ctx, value):
                if value == "BAD":
                    raise framework.ValidationError("bad name")
                return value

            name = PlainField(source_field="NAME", processor_func=_boom)

        self._make_staging_row("CI-1", "good", "d")
        self._make_staging_row("CI-2", "BAD", "d")

        factory = MappingProcessorFactory(
            repository=StagingRepository("StagingSystem"),
            default_processor_class=FlakyProcessor,  
        )
        ImportOrchestrator(factories=[factory], tr=self.tr).run()

        self.assertEqual(factory.processed_count, 1)
        self.assertEqual(factory.failed_count, 1)

    def test_orchestrator_commits_transaction(self):
        self._make_staging_row("CI-1", "Server A", "desc")
        self._build_orchestrator().run()
        # run() commits once for the import phase and once for reconciliation.
        self.assertGreaterEqual(self.tr.commit_count, 1)


if __name__ == "__main__":
    unittest.main()
