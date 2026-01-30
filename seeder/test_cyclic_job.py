import os
import sys
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, UTC


class TestCyclicJobConfiguration:
    """Test configuration and environment variable loading"""

    def test_default_environment_variables(self):
        """Test that default environment variables are set correctly"""
        with patch.dict(os.environ, {}, clear=True):
            with patch('sqlalchemy.create_engine') as mock_create_engine, \
                 patch('faker.Faker'):
                mock_engine = Mock()
                mock_conn = MagicMock()
                mock_create_engine.return_value = mock_engine
                mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
                mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
                mock_conn.execute.return_value.fetchall.return_value = []
                
                if 'cyclic_job' in sys.modules:
                    del sys.modules['cyclic_job']
                
                import cyclic_job
                
                assert cyclic_job.host == "db"
                assert cyclic_job.port == "5432"
                assert cyclic_job.db == "appdb"
                assert cyclic_job.user == "app"
                assert cyclic_job.pw == "app"
                assert cyclic_job.rows == 200

    def test_custom_environment_variables(self):
        """Test that custom environment variables are used correctly"""
        env_vars = {
            "PGHOST": "testdb",
            "PGPORT": "5432",
            "PGDATABASE": "testappdb",
            "PGUSER": "testuser",
            "PGPASSWORD": "testpass",
            "ROWS": "10"
        }
        
        with patch.dict(os.environ, env_vars):
            with patch('sqlalchemy.create_engine') as mock_create_engine, \
                 patch('faker.Faker'):
                mock_engine = Mock()
                mock_conn = MagicMock()
                mock_create_engine.return_value = mock_engine
                mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
                mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
                mock_conn.execute.return_value.fetchall.return_value = []
                
                if 'cyclic_job' in sys.modules:
                    del sys.modules['cyclic_job']
                
                import cyclic_job
                
                assert cyclic_job.host == "testdb"
                assert cyclic_job.db == "testappdb"
                assert cyclic_job.user == "testuser"
                assert cyclic_job.rows == 10

    def test_database_url_format(self):
        """Test that database URL is formatted correctly"""
        expected_url = "postgresql+psycopg2://testuser:testpass@testdb:5432/testappdb"
        
        env_vars = {
            "PGHOST": "testdb",
            "PGPORT": "5432",
            "PGDATABASE": "testappdb",
            "PGUSER": "testuser",
            "PGPASSWORD": "testpass",
            "ROWS": "10"
        }
        
        with patch.dict(os.environ, env_vars):
            with patch('sqlalchemy.create_engine') as mock_create_engine, \
                 patch('faker.Faker'):
                mock_engine = Mock()
                mock_conn = MagicMock()
                mock_create_engine.return_value = mock_engine
                mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
                mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
                mock_conn.execute.return_value.fetchall.return_value = []
                
                if 'cyclic_job' in sys.modules:
                    del sys.modules['cyclic_job']
                
                import cyclic_job
                
                mock_create_engine.assert_called_once_with(expected_url, pool_pre_ping=True)


class TestCyclicJobDatabaseOperations:
    """Test database operations"""

    def test_no_table_creation(self):
        """Test that cyclic_job does not create tables (unlike seed.py)"""
        with patch('sqlalchemy.create_engine') as mock_create_engine, \
             patch('faker.Faker') as mock_faker_class:
            mock_engine = Mock()
            mock_conn = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
            mock_conn.execute.return_value.fetchall.return_value = []
            
            mock_fake = Mock()
            mock_faker_class.return_value = mock_fake
            mock_fake.name.return_value = "Test User"
            mock_fake.unique.email.return_value = "test@example.com"
            mock_fake.word.return_value = "product"
            
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            
            import cyclic_job
            
            calls = mock_conn.execute.call_args_list
            call_args = [str(call[0][0]) for call in calls]
            has_create_table = any('CREATE TABLE' in arg for arg in call_args)
            
            assert not has_create_table, "cyclic_job should not create tables"

    def test_user_data_generation(self):
        """Test that user data is generated correctly"""
        with patch.dict(os.environ, {"ROWS": "2"}), \
             patch('sqlalchemy.create_engine') as mock_create_engine, \
             patch('faker.Faker') as mock_faker_class:
            mock_engine = Mock()
            mock_conn = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
            mock_conn.execute.return_value.fetchall.return_value = [(1,), (2,)]
            
            mock_fake = Mock()
            mock_faker_class.return_value = mock_fake
            mock_fake.name.return_value = "Test User"
            mock_fake.unique.email.return_value = "test@example.com"
            mock_fake.word.return_value = "product"
            
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            
            import cyclic_job
            
            assert mock_fake.name.call_count >= 2
            assert mock_fake.unique.email.call_count >= 2

    def test_conflict_handling(self):
        """Test that ON CONFLICT clause is used for users"""
        with patch.dict(os.environ, {"ROWS": "1"}), \
             patch('sqlalchemy.create_engine') as mock_create_engine, \
             patch('faker.Faker') as mock_faker_class:
            mock_engine = Mock()
            mock_conn = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
            mock_conn.execute.return_value.fetchall.return_value = [(1,)]
            
            mock_fake = Mock()
            mock_faker_class.return_value = mock_fake
            mock_fake.name.return_value = "Test User"
            mock_fake.unique.email.return_value = "test@example.com"
            mock_fake.word.return_value = "product"
            
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            
            import cyclic_job
            
            calls = mock_conn.execute.call_args_list
            call_args = [str(call[0][0]) for call in calls]
            has_conflict_clause = any('ON CONFLICT' in arg for arg in call_args)
            
            assert has_conflict_clause, "ON CONFLICT clause not found in user insert"

    def test_all_users_query(self):
        """Test that all users are queried from database"""
        with patch.dict(os.environ, {"ROWS": "1"}), \
             patch('sqlalchemy.create_engine') as mock_create_engine, \
             patch('faker.Faker') as mock_faker_class:
            mock_engine = Mock()
            mock_conn = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
            mock_conn.execute.return_value.fetchall.return_value = [(1,), (2,), (3,)]
            
            mock_fake = Mock()
            mock_faker_class.return_value = mock_fake
            mock_fake.name.return_value = "Test User"
            mock_fake.unique.email.return_value = "test@example.com"
            mock_fake.word.return_value = "product"
            
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            
            import cyclic_job
            
            calls = mock_conn.execute.call_args_list
            call_args = [str(call[0][0]) for call in calls]
            has_user_query = any('SELECT id FROM users' in arg for arg in call_args)
            
            assert has_user_query, "SELECT id FROM users query not found"


class TestCyclicJobDataValidation:
    """Test data validation and integrity"""

    def test_order_amount_range(self):
        """Test that order amounts are within expected range"""
        with patch.dict(os.environ, {"ROWS": "1"}), \
             patch('sqlalchemy.create_engine') as mock_create_engine, \
             patch('faker.Faker') as mock_faker_class, \
             patch('random.randint') as mock_randint, \
             patch('random.uniform') as mock_uniform:
            mock_engine = Mock()
            mock_conn = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
            mock_conn.execute.return_value.fetchall.return_value = [(1,)]
            
            mock_fake = Mock()
            mock_faker_class.return_value = mock_fake
            mock_fake.name.return_value = "Test User"
            mock_fake.unique.email.return_value = "test@example.com"
            mock_fake.word.return_value = "product"
            
            mock_randint.return_value = 2
            mock_uniform.return_value = 150.50
            
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            
            import cyclic_job
            
            calls = mock_uniform.call_args_list
            if calls:
                assert any(call[0] == (10, 500) for call in calls)

    def test_orders_per_user_range(self):
        """Test that orders per user are within expected range (1-3)"""
        with patch.dict(os.environ, {"ROWS": "1"}), \
             patch('sqlalchemy.create_engine') as mock_create_engine, \
             patch('faker.Faker') as mock_faker_class, \
             patch('random.randint') as mock_randint:
            mock_engine = Mock()
            mock_conn = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
            mock_conn.execute.return_value.fetchall.return_value = [(1,)]
            
            mock_fake = Mock()
            mock_faker_class.return_value = mock_fake
            mock_fake.name.return_value = "Test User"
            mock_fake.unique.email.return_value = "test@example.com"
            mock_fake.word.return_value = "product"
            
            mock_randint.return_value = 2
            
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            
            import cyclic_job
            
            calls = mock_randint.call_args_list
            assert any(call[0] == (1, 3) for call in calls)


class TestCyclicJobOutputAndLogging:
    """Test output and logging"""

    def test_success_message(self):
        """Test that success message is printed"""
        with patch.dict(os.environ, {"ROWS": "1"}), \
             patch('sqlalchemy.create_engine') as mock_create_engine, \
             patch('faker.Faker') as mock_faker_class, \
             patch('builtins.print') as mock_print:
            mock_engine = Mock()
            mock_conn = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
            mock_conn.execute.return_value.fetchall.return_value = [(1,), (2,)]
            
            mock_fake = Mock()
            mock_faker_class.return_value = mock_fake
            mock_fake.name.return_value = "Test User"
            mock_fake.unique.email.return_value = "test@example.com"
            mock_fake.word.return_value = "product"
            
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            
            import cyclic_job
            
            assert mock_print.called
            print_args = [str(call[0][0]) for call in mock_print.call_args_list]
            has_success_msg = any('Users total' in arg for arg in print_args)
            assert has_success_msg, "Success message not printed"
