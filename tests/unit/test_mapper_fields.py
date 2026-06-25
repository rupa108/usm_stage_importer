import unittest
from mock import MagicMock, patch
from tests.lib import test_support

VM = test_support.bootstrap()
VM.configure_type("ExampleBO", business_key_attr="pk", object_link_fields=["type"])
VM.configure_type("TypeBO", business_key_attr="type_name")

from stage_importer_framework import (
    MappingProcessor,
    PlainField,
    StaticField,
    RelationField,
    ChainedRelationField,
    CLEAR_LINK,
    undefined,
    Static,
    FromSource,
    FromAnywhere,
)


class TestMapperFields(unittest.TestCase):

    def setUp(self):
        self.vm = VM
        self.tr = test_support.reset_environment(self.vm)

    def test_plain_field(self):
        field = PlainField(source_field="source", match_key=True)
        self.assertEqual(field.source_field, "source")
        self.assertTrue(field.match_key)

    def test_plain_field_get_processed_value(self):
        context = MagicMock()
        field = PlainField(source_field="source", processor_func=lambda ctx, v: v.upper())
        
        # Mock the source BO field
        source_bo = MagicMock()
        source_field = MagicMock()
        source_field.getValue.return_value = "test"
        source_bo.getBOField.return_value = source_field
        context.get_source.return_value = source_bo
        
        value = field.get_processed_value(context)
        self.assertEqual(value, "TEST")
        
        # Test without processor_func
        field = PlainField(source_field="source")
        value = field.get_processed_value(context)
        self.assertEqual(value, "test")

    def test_plain_field_map_value(self):
        context = MagicMock()
        field = PlainField(source_field="source", processor_func=lambda ctx, v: v.upper())
        
        # Mock the source BO field
        source_bo = MagicMock()
        source_field = MagicMock()
        source_field.getValue.return_value = "test"
        source_bo.getBOField.return_value = source_field
        context.get_source.return_value = source_bo
        
        field.map_value(context)
        context.store_value.assert_called_once_with(field, "TEST")

    def test_plain_field_set_target_value(self):
        context = MagicMock()
        field = PlainField(source_field="source")
        field.set_target_field("target")
        
        # Test with defined value
        context.get_value.return_value = "test_value"
        target_bo = MagicMock()
        target_field = MagicMock()
        target_bo.getBOField.return_value = target_field
        context.get_target.return_value = target_bo
        
        field.set_target_value(context)
        target_field.setValue.assert_called_once_with("test_value")
        
        # Test with undefined value
        context.reset_mock()
        context.get_value.return_value = undefined
        field.set_target_value(context)
        target_field.setValue.assert_not_called()

    def test_static_field(self):
        field = StaticField(value="static_value")
        self.assertEqual(field.value, "static_value")
        
        # Test with processor_func
        field = StaticField(value="static_value", processor_func=lambda ctx, v: v.upper())
        self.assertEqual(field.value, "static_value")

    def test_static_field_get_processed_value(self):
        context = MagicMock()
        
        # Test with value and no processor_func
        field = StaticField(value="static_value")
        value = field.get_processed_value(context)
        self.assertEqual(value, "static_value")
        
        # Test with processor_func
        field = StaticField(value="static_value", processor_func=lambda ctx, v: v.upper())
        value = field.get_processed_value(context)
        self.assertEqual(value, "STATIC_VALUE")
        
        # Test with processor_func only
        field = StaticField(processor_func=lambda ctx, v: "generated_value")
        value = field.get_processed_value(context)
        self.assertEqual(value, "generated_value")

    def test_static_field_map_value(self):
        context = MagicMock()
        
        # Test with value
        field = StaticField(value="static_value")
        field.map_value(context)
        context.store_value.assert_called_once_with(field, "static_value")
        
        # Test with processor_func
        context.reset_mock()
        field = StaticField(value="static_value", processor_func=lambda ctx, v: v.upper())
        field.map_value(context)
        context.store_value.assert_called_once_with(field, "STATIC_VALUE")

    def test_relation_field(self):
        field = RelationField(source_field="source", target_bo_name="TargetBO")
        self.assertEqual(field.source_field, "source")
        self.assertEqual(field.target_bo_name, "TargetBO")

    def test_relation_field_get_lookup_value(self):
        context = MagicMock()
        field = RelationField(source_field="source", target_bo_name="TargetBO")
        
        # Mock the source BO field
        source_bo = MagicMock()
        source_field = MagicMock()
        source_field.getValue.return_value = "lookup_value"
        source_bo.getBOField.return_value = source_field
        context.get_source.return_value = source_bo
        
        value = field.get_lookup_value(context)
        self.assertEqual(value, "lookup_value")

    @patch('stage_importer_framework.get_bo')
    def test_relation_field_get_processed_value(self, mock_get_bo):
        context = MagicMock()
        tr = MagicMock()
        context.get_transaction.return_value = tr
        
        # Mock the source BO field
        source_bo = MagicMock()
        source_field = MagicMock()
        source_field.getValue.return_value = "lookup_value"
        source_bo.getBOField.return_value = source_field
        context.get_source.return_value = source_bo
        
        # Test with target_lookup_field
        field = RelationField(
            source_field="source", 
            target_bo_name="TargetBO",
            target_lookup_field="name"
        )
        
        # Mock target BO
        target_bo = MagicMock()
        mock_get_bo.return_value = target_bo
        
        value = field.get_processed_value(context)
        self.assertEqual(value, target_bo)
        mock_get_bo.assert_called_once_with(tr, field.target_type, "name == 'lookup_value'", strict=True)
        
        # Test with CLEAR_LINK
        mock_get_bo.reset_mock()
        source_field.getValue.return_value = None
        field = RelationField(
            source_field="source", 
            target_bo_name="TargetBO",
            target_lookup_field="name"
        )
        
        value = field.get_processed_value(context)
        self.assertEqual(value, CLEAR_LINK)
        
        # Test with processor_func
        mock_get_bo.reset_mock()
        source_field.getValue.return_value = "lookup_value"
        field = RelationField(
            source_field="source", 
            target_bo_name="TargetBO",
            processor_func=lambda ctx, v: "processed_value"
        )
        
        value = field.get_processed_value(context)
        self.assertEqual(value, "processed_value")

    @patch('stage_importer_framework.get_bo')
    def test_relation_field_map_value(self, mock_get_bo):
        context = MagicMock()
        tr = MagicMock()
        context.get_transaction.return_value = tr
        
        # Mock the source BO field
        source_bo = MagicMock()
        source_field = MagicMock()
        source_field.getValue.return_value = "lookup_value"
        source_bo.getBOField.return_value = source_field
        context.get_source.return_value = source_bo
        
        # Test with target_lookup_field
        field = RelationField(
            source_field="source", 
            target_bo_name="TargetBO",
            target_lookup_field="name"
        )
        
        # Mock target BO
        target_bo = MagicMock()
        mock_get_bo.return_value = target_bo
        
        field.map_value(context)
        context.store_value.assert_called_once_with(field, target_bo)

    @patch('stage_importer_framework.link_nm')
    def test_relation_field_set_target_value(self, mock_link_nm):
        context = MagicMock()
        
        # Test with ObjectLink
        field = RelationField(source_field="source", target_bo_name="TargetBO")
        field.set_target_field("target")
        target_bo = MagicMock()
        target_field = MagicMock()
        target_field.isObjectLink.return_value = True
        target_field.isCollectionLink.return_value = False
        target_bo.getBOField.return_value = target_field
        context.get_target.return_value = target_bo
        
        # Test with related BO
        context.get_value.return_value = MagicMock()
        field.set_target_value(context)
        target_field.setObject.assert_called_once()
        
        # Test with CLEAR_LINK
        context.reset_mock()
        target_field.reset_mock()
        context.get_value.return_value = CLEAR_LINK
        field.set_target_value(context)
        target_field.setObject.assert_called_once_with(None)
        
        # Test with CollectionLink
        context.reset_mock()
        target_field.reset_mock()
        target_field.isObjectLink.return_value = True
        target_field.isCollectionLink.return_value = True
        link_bo = MagicMock()
        mock_link_nm.return_value = link_bo
        
        related_bo = MagicMock()
        context.get_value.return_value = related_bo
        
        field.set_target_value(context)
        mock_link_nm.assert_called_once_with(target_bo, related_bo, "target")
        context.add_touched_object.assert_called_with(related_bo)

    def test_chained_relation_field(self):
        class TypeMappingProcessor(MappingProcessor):
            type_name = PlainField(source_field="type_name", match_key=True)
            class Meta:
                target_bo_name = "TypeBO"
        source_bo = VM.getBOType("TypeBO").createBO()
        source_bo.getBOField("type_name").setValue("example_type")
        target_bo = VM.getBOType("ExampleBO").createBO()
        
        context = MagicMock()
        context.get_source.return_value = source_bo
        context.source_bo = source_bo
        context.get_target.return_value = target_bo
        context.target_bo = target_bo

        field = ChainedRelationField("type_name", processor_or_factory=TypeMappingProcessor)
        field.set_target_field("type")
        field.map_value(context)
        
        self.assertTrue(context.store_value.called)
        call_args = context.store_value.call_args[0]
        self.assertEqual(call_args[0], field)
        self.assertEqual(call_args[1].getBOField("type_name").getValue(), "example_type")

        context.get_value.return_value = call_args[1]
        field.set_target_value(context)
        target_field = target_bo.getBOField("type")
        self.assertEqual(target_field.getObject().getBOField("type_name").getValue(), "example_type")


    def test_value_source_classes(self):
        context = MagicMock()
        
        # Test Static
        static_source = Static("static_value")
        self.assertEqual(static_source.get_value(context), "static_value")
        
        # Test Static with processor_func
        static_source = Static("static_value", processor_func=lambda ctx, v: v.upper())
        self.assertEqual(static_source.get_value(context), "STATIC_VALUE")
        
        # Test FromSource
        source_bo = MagicMock()
        source_field = MagicMock()
        source_field.getValue.return_value = "source_value"
        source_bo.getBOField.return_value = source_field
        context.get_source.return_value = source_bo
        
        from_source = FromSource("source_field")
        self.assertEqual(from_source.get_value(context), "source_value")
        
        # Test FromSource with processor_func
        from_source = FromSource("source_field", processor_func=lambda ctx, v: v.upper())
        self.assertEqual(from_source.get_value(context), "SOURCE_VALUE")
        
        # Test FromAnywhere
        from_anywhere = FromAnywhere(lambda ctx, v: "anywhere_value")
        self.assertEqual(from_anywhere.get_value(context), "anywhere_value")


if __name__ == "__main__":
    unittest.main()