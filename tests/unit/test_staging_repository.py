# -*- coding: utf-8 -*-
"""Integration tests for the StagingRepository class."""

import unittest
from mock import patch
from tests.lib import test_support

# Stub out the USM imports and seed the global VM BEFORE importing the framework.
VM = test_support.bootstrap()

from stage_importer_framework import StagingRepository

class TestStagingRepositoryIntegration(unittest.TestCase):
    """Integration tests for the StagingRepository class."""

    def setUp(self):
        """Set up the test environment with fake VM and self.tr."""

        # Configure the fake VM with our staging BO type
        self.staging_bo_name = "StagingSystem1"
        #VM.configure_type(self.staging_bo_name)
        self.vm = VM
        self.tr = test_support.reset_environment(self.vm)
        # Create some test data
        self.test_data = [
            {"CI_NR": "CI-1", "NAME": "Server A", "DESCRIPTION": "Primary server", "status": "PENDING"},
            {"CI_NR": "CI-2", "NAME": "Server B", "DESCRIPTION": "Backup server", "status": "PENDING"},
            {"CI_NR": "CI-3", "NAME": "Server C", "DESCRIPTION": "Test server", "status": "PROCESSED"},
        ]
        self._bos = []
        # Add test data to the fake VM store
        for data in self.test_data:
            self._bos.append(self._createBO(self.staging_bo_name, data))

    
    def _createBO(self, bo_type_name, initial_dict=None):
        bo = self.vm.getBOType(bo_type_name).createBO()
        if initial_dict:
            for field_name, value in initial_dict.items():
                bo.getBOField(field_name).setValue(value)
        return bo
    
    def test_get_unprocessed_records_integration(self):
        """Test that get_unprocessed_records returns the correct records in an integration scenario."""
        # Create a staging repository with a condition
        condition = "status == 'PENDING'"
        repo = StagingRepository(self.staging_bo_name, condition)

        # Get the unprocessed records
        iterator = repo.get_unprocessed_records(self.tr)

        # Convert the iterator to a list for easier testing
        records = list(iterator)

        # We expect 2 records with status "PENDING"
        self.assertEqual(len(records), 2)

        # Check that the records have the correct data
        for record in records:
            self.assertEqual(record.getBOField("status").getValue(), "PENDING")
            self.assertIn(record.getBOField("CI_NR").getValue(), ["CI-1", "CI-2"])

    def test_get_unprocessed_records_empty_condition_integration(self):
        """Test that get_unprocessed_records works with an empty condition in an integration scenario."""
        # Create a staging repository with an empty condition
        repo = StagingRepository(self.staging_bo_name, "")

        # Get the unprocessed records
        iterator = repo.get_unprocessed_records(self.tr)

        # Convert the iterator to a list for easier testing
        records = list(iterator)

        # We expect all 3 records since there's no condition
        self.assertEqual(len(records), 3)

        # Check that we have all the test data
        expected_ci_nrs = ["CI-1", "CI-2", "CI-3"]
        for record in records:
            self.assertIn(record.getBOField("CI_NR").getValue(), expected_ci_nrs)

    def test_get_unprocessed_records_with_no_matches_integration(self):
        """Test that get_unprocessed_records handles cases with no matching records."""
        # Create a staging repository with a condition that won't match any records
        condition = "status == 'COMPLETED'"
        repo = StagingRepository(self.staging_bo_name, condition)

        # Get the unprocessed records
        iterator = repo.get_unprocessed_records(self.tr)

        # Convert the iterator to a list for easier testing
        records = list(iterator)

        # We expect 0 records
        self.assertEqual(len(records), 0)

if __name__ == "__main__":
    unittest.main()