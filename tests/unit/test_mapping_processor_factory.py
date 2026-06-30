import unittest
from mock import MagicMock, Mock, call
from tests.lib import test_support

VM = test_support.bootstrap()
VM.configure_type("ExampleBO", business_key_attr="pk", object_link_fields=["type"])

from stage_importer_framework import (
    MappingProcessorFactory,
    MappingProcessor,
    PlainField,
    StaticField,
    RelationField,
    StagingRepository,
    ProcessingContext,
)

global transaction

class ExampleProcessor(MappingProcessor):
    pk = PlainField(source_field="ID", match_key=True)
    name = PlainField(source_field="NAME")
    status = StaticField(value="ACTIVE")
    type = RelationField(source_field="TYPE", target_bo_name="TypeBO", target_lookup_field="type_name")

    class Meta:
        target_bo_name = "ExampleBO"


class TestMappingProcessorFactory(unittest.TestCase):
    def setUp(self):
        self.vm = VM
        self.tr = test_support.reset_environment(self.vm)        
        self.existing_bo1 = self._createBO("ExampleBO", {'pk': 'CI-10-2340943', 'name': 'MyCI1'})
        self.existing_bo2 = self._createBO("ExampleBO", {'pk': 'CI-10-2340944', 'name': 'MyCI2'})
        self.row_bo1 = self._createBO("ExampleStagingBO", {'ID': 'CI-10-2340943', 'NAME': 'MyCI1',"TYPE": "alt"})
        self.row_bo2 = self._createBO("ExampleStagingBO", {'ID': 'CI-10-2340944', 'NAME': 'MyCI2',"TYPE": "default"})
        self.row_bo3 = self._createBO("ExampleStagingBO", {'ID': 'CI-10-234095x', 'NAME': 'SomeCI',"TYPE": "some"})

        self.factory = MappingProcessorFactory(
            repository=StagingRepository(staging_bo_name="ExampleStagingBO"),
            default_processor_class=ExampleProcessor,
        )
        
    def tearDown(self):
        self.vm.reset()

    def _createBO(self, bo_type_name, initial_dict=None):
        bo = VM.getBOType(bo_type_name).createBO()
        if initial_dict:
            for field_name, value in initial_dict.items():
                bo.getBOField(field_name).setValue(value)
        return bo
        
    def test_create_processor(self):

        self.assertEqual(self.factory.repository.staging_bo_name, "ExampleStagingBO")
        self.assertEqual(self.factory.default_processor_class, ExampleProcessor)
        self.assertEqual(len(self.factory.rules), 0)

    def test_create_processor_with_rules(self):
        rule = (lambda x: True, ExampleProcessor)
        factory = MappingProcessorFactory(
            repository=StagingRepository(staging_bo_name="ExampleStagingBO"),
            rules=[rule],
        )
        self.assertEqual(len(factory.rules), 1)
        self.assertIn(rule, factory.rules)

    def test_create_processor_with_invalid_rule(self):
        with self.assertRaises(AssertionError):
            MappingProcessorFactory(
                repository=StagingRepository(staging_bo_name="ExampleStagingBO"),
                rules=[("not a callable", ExampleProcessor)],
            )

    def test_create_processor_with_invalid_processor_class(self):
        with self.assertRaises(TypeError):
            MappingProcessorFactory(
                repository=StagingRepository(staging_bo_name="ExampleStagingBO"),
                rules=[(lambda x: True, "not a processor class")],
            )

    def test_create_processor_with_invalid_repository(self):
        with self.assertRaises(AssertionError):
            MappingProcessorFactory(
                repository="not a repository",
                rules=[(lambda x: True, ExampleProcessor)],
            )
       
    def test_create_processor_with_missing_match_key(self):
        class AlternativeProcessor(MappingProcessor):
            pk = PlainField("CI_NR")
            name = PlainField("SOURCE_NAME")
            class Meta:
                target_bo_name = "AternativeBO"       
        with self.assertRaises(AssertionError):
            MappingProcessorFactory(
                repository=StagingRepository(staging_bo_name="ExampleStagingBO"),
                default_processor_class=AlternativeProcessor,
            )   
    def test_create_processor_with_missing_target_bo_name(self):
        class AlternativeProcessor(MappingProcessor):
            pk = PlainField("CI_NR", match_key=True)
            name = PlainField("SOURCE_NAME")
        with self.assertRaises(AssertionError):
            MappingProcessorFactory(
                    repository=StagingRepository(staging_bo_name="ExampleStagingBO"),
                    default_processor_class=AlternativeProcessor,
            )                  
    def test_get_processor_without_rules(self):
        
        source_bo = MagicMock()
        target_bo = MagicMock()
        processor_class = self.factory._get_processor_class(transaction, source_bo)
        self.assertEqual(processor_class, ExampleProcessor)
        processor = self.factory.build_processor(transaction, processor_class, source_bo, target_bo)
        self.assertIsInstance(processor, processor_class)
        self.assertEqual(processor.source, source_bo)
        self.assertEqual(processor.target, target_bo)

    def test_get_processor_with_rules(self):
        class AlternativeProcessor(MappingProcessor):
            pk = PlainField("CI_NR", match_key=True)
            name = PlainField("SOURCE_NAME")
            class Meta:
                target_bo_name = "AternativeBO"

        factory = MappingProcessorFactory(
            repository=StagingRepository(staging_bo_name="ExampleStagingBO"),
            default_processor_class=ExampleProcessor,
            rules=[(lambda row_bo: row_bo.getBOField("TYPE").getValue() == 'alt', AlternativeProcessor)]
        )
        alt_processor_class = factory._get_processor_class(transaction, self.row_bo1) 
        default_processor_class = factory._get_processor_class(transaction, self.row_bo2)
        self.assertEqual(alt_processor_class, AlternativeProcessor)
        self.assertEqual(default_processor_class, ExampleProcessor)

    def test_get_source_bo(self):

        row_bo = MagicMock()      
        result = self.factory.get_source_bo(transaction, row_bo)
        
        self.assertEqual(result, row_bo)

    def test_get_target_bo(self):

        result = self.factory.get_target_bo(transaction, self.row_bo1, ExampleProcessor) 
        self.assertEqual(result, self.existing_bo1)
        
        result = self.factory.get_target_bo(transaction, self.row_bo3, ExampleProcessor) 
        self.assertIsNone(result)

    def test_get_or_create_target(self):
        factory = MappingProcessorFactory(
            repository=StagingRepository(staging_bo_name="ExampleStagingBO"),
            default_processor_class=ExampleProcessor,
        )
        factory.generate_key = False
        
        result_bo, created = factory.get_or_create_target(transaction, self.row_bo1, ExampleProcessor)
            
        self.assertEqual(result_bo, self.existing_bo1)
        self.assertFalse(created)
        
        result_bo, created = factory.get_or_create_target(transaction, self.row_bo3, ExampleProcessor)
        new_pk = result_bo.getBOField("pk").getValue()
        self.assertIsNone(new_pk)
        self.assertTrue(created)
        
        factory.generate_key = True
        result_bo, created = factory.get_or_create_target(transaction, self.row_bo3, ExampleProcessor)
        new_pk = result_bo.getBOField("pk").getValue()
        self.assertTrue(new_pk.startswith("KEY-"))

    def test_process_row(self):
        type_bo1 = self._createBO("TypeBO", {"type_name": "alt"})
        result, created = self.factory.process_row(transaction, self.row_bo1)
        self.assertEqual(result, self.existing_bo1)
        type_bo2 = result.getBOField("type").getObject()
        self.assertEqual(type_bo1, type_bo2)


    def test_process_chained(self):
        """
        Test the process_chained method of MappingProcessorFactory.
        """
        factory = MappingProcessorFactory.as_chained(
            default_processor_class=ExampleProcessor,
        )
        
        type_bo1 = self._createBO("TypeBO", {"type_name": "alt"})
        processor = ExampleProcessor(transaction, self.row_bo1, self.existing_bo1)
        context = ProcessingContext(processor, source_field_name="ID", target_field_name="pk")
        
        result = factory.process_chained(context)
        
        self.assertEqual(result, self.existing_bo1)
        type_bo2 = result.getBOField("type").getObject()
        self.assertEqual(type_bo1, type_bo2)
    
class TestMappingProcessorFactoryHooks(unittest.TestCase):

    def tearDown(self):
        VM.reset()

    def test_factory_lifecycle_hooks_are_called_in_correct_order(self):
        
        mock_record = Mock(name="StagingRecord")
        mock_record.getMoniker.return_value = "Staging_BO_1"
        
        mock_target_bo = Mock(name="TargetBusinessObject")
        mock_target_bo.getMoniker.return_value = "Target_BO_1"

        mock_repo = Mock(spec=StagingRepository)
        mock_repo.get_unprocessed_records.return_value = [mock_record]

        factory = MappingProcessorFactory(
            repository=mock_repo,
            default_processor_class=ExampleProcessor,
        )

        factory.get_or_create_target = Mock(return_value=(mock_target_bo, True))
        factory._mark_as_processed = Mock()

        mock_processor_instance = MagicMock(name="ProcessorInstance")

        spy_manager = Mock()

        factory.prepare_source_record = Mock(return_value=mock_record)
        factory.should_process = Mock(return_value=True)
        factory.on_target_created = Mock()
        factory.build_processor = Mock(return_value=mock_processor_instance)

        spy_manager.attach_mock(factory.prepare_source_record, 'prepare_source_record')
        spy_manager.attach_mock(factory.should_process, 'should_process')
        spy_manager.attach_mock(factory.on_target_created, 'on_target_created')
        spy_manager.attach_mock(factory.build_processor, 'build_processor')

        factory.process_all(transaction)

        factory.prepare_source_record.assert_called_once()
        factory.should_process.assert_called_once()
        factory.on_target_created.assert_called_once()
        factory.build_processor.assert_called_once()


        expected_calls_sequence = [
            call.prepare_source_record(transaction, mock_record),
            call.should_process(transaction, mock_record),
            call.on_target_created(transaction, mock_target_bo, mock_record),
            call.build_processor(transaction, ExampleProcessor, mock_record, mock_target_bo, is_create=True)
        ]
        
        spy_manager.assert_has_calls(expected_calls_sequence, any_order=False)


    def test_factory_lifecycle_with_existing_target(self):
        mock_record = Mock(name="StagingRecord")
        mock_target_bo = Mock(name="TargetBusinessObject")
        
        mock_repo = Mock(spec=StagingRepository)
        mock_repo.get_unprocessed_records.return_value = [mock_record]

        factory = MappingProcessorFactory(mock_repo, default_processor_class=ExampleProcessor)
        
        factory.get_or_create_target = Mock(return_value=(mock_target_bo, False))
        factory._skip_update = Mock(return_value=False)
        factory._mark_as_processed = Mock()
        factory.build_processor = Mock(return_value=MagicMock())

        factory.on_target_found = Mock()

        factory.process_all(transaction)

        factory.on_target_found.assert_called_once_with(transaction, mock_target_bo, mock_record)

if __name__ == '__main__':
    unittest.main()
        


      
