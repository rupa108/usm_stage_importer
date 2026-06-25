from tests.lib import test_support
import unittest

VM = test_support.bootstrap()

class TestFakeBo(unittest.TestCase):
    def setUp(self):
        self.vm = VM
        self.tr = test_support.reset_environment(self.vm)
    
    def test_create_bo(self):
        bo = self.vm.create_bo("System", systemname="S-00001", name="system1")
        self.assertEqual(bo.getBOField("systemname").getValue(), "S-00001")
        self.assertEqual(bo.getBOField("name").getValue(), "system1")

    def test_create_bo_with_relation(self):
        parent = self.vm.create_bo("System", systemname="S-00001", name="system1")
        child = self.vm.create_bo("System", systemname="S-00002", name="system2", parent_system=parent)
        self.assertEqual(child.getBOField("parent_system").getObject(), parent)

    def test_create_bo_with_nm_realtion(self):
        outgoing = self.vm.create_bo("System", systemname="S-00001", name="system1")
        system = self.vm.create_bo("System", systemname="S-00002", name="system2", outgoing_systems=[outgoing])
        self.assertEqual(system.getBOField("outgoing_systems").getCollection().indexOf(outgoing), 0)
        self.assertEqual(system.getBOField("outgoing_systems").getCollection().get(0), outgoing)

    def test_find_bo(self):
        pk = "S-00001"
        self.vm.create_bo("System", systemname=pk, name="system1")
        search_result = self.vm.getBOType("System").find(self.tr,"systemname == '%s'" % pk)
        self.assertTrue(search_result.isOne())
        self.assertFalse(search_result.isMore())
        self.assertFalse(search_result.isEmpty())
        bo = search_result.getBObject()
        self.assertEqual(bo.getBOField("systemname").getValue(), pk)
        self.assertEqual(bo.getBOField("name").getValue(), "system1")
        
    def test_link_nm(self):
        source = self.vm.create_bo("System", systemname="S-00001", name="system1")
        target = self.vm.create_bo("System", systemname="S-00002", name="system2")
        from stage_importer_framework import link_nm
        link_bo = link_nm(source, target, "outgoing_systems", usagetype="test")
        self.assertFalse(source.getBOField("outgoing_systems").getCollection().isEmpty())
        self.assertEqual(source.getBOField("outgoing_systems").getCollection().size(), 1)
        self.assertEqual(source.getBOField("outgoing_systems").getCollection().get(0), target)
        self.assertEqual(link_bo.getBOField("usagetype").getValue(), "test")



                               