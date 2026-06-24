import unittest
from mock import MagicMock, patch
from tests.lib import test_support

VM = test_support.bootstrap()

from stage_importer_framework import (
    PlainField,
    StaticField,
    RelationField,
    MappingProcessor
)

class ExampleProcessor(MappingProcessor):
    pk = PlainField(source_field="ID", match_key=True)
    name = PlainField(source_field="NAME")
    status = StaticField(value="ACTIVE")
    type = RelationField(source_field="TYPE", target_bo_name="TypeBO", target_lookup_field="type_name")

    class Meta:
        target_bo_name = "ExampleBO"

class ExampleProcessor2(ExampleProcessor):
    __processing_order__ = ("pk", "status", "name", "type")

class TestMappingProcessor(unittest.TestCase):

    def _createBO(self, bo_type_name, initial_dict=None):
        bo = VM.getBOType(bo_type_name).createBO()
        if initial_dict:
            for field_name, value in initial_dict.items():
                bo.getBOField(field_name).setValue(value)
        return bo
    def test_meta_class_processing(self):
        # Ensure that the Meta class is processed correctly and fields are collected     
        self.assertEqual(ExampleProcessor.meta.target_bo_name, "ExampleBO")
        self.assertListEqual(
            [field.target_field for field in ExampleProcessor.meta.fields], 
            ["pk", "name", "status", "type"]
        )
        self.assertEqual(ExampleProcessor.meta.fields[0].target_field, "pk")
        self.assertEqual(ExampleProcessor.meta.fields[1].target_field, "name")
        self.assertEqual(ExampleProcessor.meta.fields[2].target_field, "status")
        self.assertEqual(ExampleProcessor.meta.fields[3].target_field, "type")
        self.assertEqual(ExampleProcessor.meta.target_bo_name, "ExampleBO")

        self.assertEqual(ExampleProcessor2.meta.fields[0].target_field, "pk")
        self.assertEqual(ExampleProcessor2.meta.fields[1].target_field, "status")
        self.assertEqual(ExampleProcessor2.meta.fields[2].target_field, "name")
        self.assertEqual(ExampleProcessor2.meta.fields[3].target_field, "type")
        self.assertEqual(ExampleProcessor2.meta.target_bo_name, "ExampleBO")
       
    def test_process(self):
        VM.configure_type("TestTargetType", business_key_attr="id", object_link_fields=["type"])
        source_bo = self._createBO("TestSourceType", {"ID": "123", "NAME": "Test Name", "TYPE": "TestType"})
        test_type_bo = self._createBO("TypeBO", {"type_name": "TestType"})
        target_bo = self._createBO("TestTargetType")
        
        processor = ExampleProcessor(transaction, source_bo, target_bo)
        processor.process()
             
        # Verify that fields were mapped
        self.assertEqual(target_bo.getBOField("pk").getValue(), "123")
        self.assertEqual(target_bo.getBOField("name").getValue(), "Test Name")
        self.assertEqual(target_bo.getBOField("status").getValue(), "ACTIVE")
        self.assertEqual(target_bo.getBOField("type").getObject(), test_type_bo)  

    def test_pre_process_and_post_process(self):
        source_bo = MagicMock()
        target_bo = MagicMock()
        
        class CustomProcessor(ExampleProcessor):
            def pre_process(self):
                self.pre_process_called = True
            
            def post_process(self):
                self.post_process_called = True
        
        processor = CustomProcessor(transaction, source_bo, target_bo)
        processor.pre_process()
        processor.post_process()
        
        self.assertTrue(processor.pre_process_called)
        self.assertTrue(processor.post_process_called)

    def test_add_touched_object(self):
        source_bo = MagicMock()
        target_bo = MagicMock()
        processor = ExampleProcessor(transaction, source_bo, target_bo)
        
        touched_bo = self._createBO("TestType")
        processor.add_touched_object(touched_bo)
        self.assertIn(touched_bo.getMoniker(), processor.get_active_keys())

    def test_get_active_keys(self):
        source_bo = self._createBO("TestSourceType")
        target_bo = self._createBO("TestTargetType")
        processor = ExampleProcessor(transaction, source_bo, target_bo)
        
        # Add a touched object
        touched_bo = self._createBO("TestTouchedType")
        processor.add_touched_object(touched_bo)
        
        active_keys = processor.get_active_keys()
        self.assertNotIn(source_bo.getMoniker(), active_keys)
        self.assertIn(target_bo.getMoniker(), active_keys)
        self.assertIn(touched_bo.getMoniker(), active_keys)

    def test_generate_key(self):
        # Test when target_type has a business key
        ExampleProcessor.meta.target_type = MagicMock()
        ExampleProcessor.meta.target_type.getBusinessKeyAttrName.return_value = "id"
        self.assertTrue(ExampleProcessor.generate_key())
        
        # Test when target_type does not have a business key
        ExampleProcessor.meta.target_type.getBusinessKeyAttrName.return_value = None
        self.assertFalse(ExampleProcessor.generate_key())

if __name__ == "__main__":
    unittest.main()