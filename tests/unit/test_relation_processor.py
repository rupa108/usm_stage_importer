import unittest
from mock import patch
from tests.lib import test_support

VM = test_support.bootstrap()
from stage_importer_framework import RelationshipProcessor

class OutgoingRelationshipProcessor(RelationshipProcessor):
    rel_attr_name = "parent_system"

class ParentRelationshipProcessor(RelationshipProcessor):
    rel_attr_name = "parent_system"

class TestRelationshipProcessor(unittest.TestCase):
    def setUp(self):
        self.vm = VM # type FakeVMApi
        self.tr = test_support.reset_environment(self.vm)
        self.source_bo = self.vm.create_bo("System", systemname="S-00001", name="system1", status="ACTIVE")
        self.target_bo = self.vm.create_bo("System", systemname="S-00002", name="system1", status="ACTIVE")
        
    def test_to_one_relation(self):
        processor = ParentRelationshipProcessor(self.tr, self.source_bo, self.target_bo)
        processor.process()
        self.assertEqual(self.source_bo.getBOField("parent_system").getObject(), self.target_bo)

    def test_to_many_relation(self):
        processor = OutgoingRelationshipProcessor(self.tr, self.source_bo, self.target_bo)
        processor.process()
        for bo in self.source_bo.getBOField("outgoing_systems").getCollection():
            self.assertEqual(bo, self.target_bo)
        
    def test_to_one_relation_without_target(self):
        processor = ParentRelationshipProcessor(self.tr, self.source_bo, None)
        processor.process()
        self.assertEqual(self.source_bo.getBOField("parent_system").getObject(), None)
