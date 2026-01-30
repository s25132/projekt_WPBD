import os
import sys
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, UTC


class TestSeedConfiguration:
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
                
                # Clear module cache
                if 'seed' in sys.modules:
                    del sys.modules['seed']
                
                import seed
                
                assert seed.host == "db"
                assert seed.port == "5432"
                assert seed.db == "appdb"
                assert seed.user == "app"
                assert seed.pw == "app"
                assert seed.rows == 200

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
                
                if 'seed' in sys.modules:
                    del sys.modules['seed']
                
                import seed
                
                assert seed.host == "testdb"
                assert seed.db == "testappdb"
                assert seed.user == "testuser"
                assert seed.rows == 10

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
                
                if 'seed' in sys.modules:
                    del sys.modules['seed']
                
                import seed
                
                mock_create_engine.assert_called_once_with(expected_url, pool_pre_ping=True)


class TestDatabaseOperations:
    """Test database operations"""

    def test_table_creation(self):
        """Test that tables are created with correct DDL"""
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
            
            if 'seed' in sys.modules:
                del sys.modules['seed']
            
            import seed
            
            calls = mock_conn.execute.call_args_list
            assert len(calls) >= 2
            
            call_args = [str(call[0][0]) for call in calls]
            has_users_table = any('CREATE TABLE IF NOT EXISTS users' in arg for arg in call_args)
            has_orders_table = any('CREATE TABLE IF NOT EXISTS orders' in arg for arg in call_args)
            
            assert has_users_table, "Users table DDL not found"
            assert has_orders_table, "Orders table DDL not found"

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
            
            if 'seed' in sys.modules:
                del sys.modules['seed']
            
            import seed
            
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
            
            if 'seed' in sys.modules:
                del sys.modules['seed']
            
            import seed
            
            calls = mock_conn.execute.call_args_list
            call_args = [str(call[0][0]) for call in calls]
            has_conflict_clause = any('ON CONFLICT' in arg for arg in call_args)
            
            assert has_conflict_clause, "ON CONFLICT clause not found in user insert"


class TestDataValidation:
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
            
            if 'seed' in sys.modules:
                del sys.modules['seed']
            
            import seed
            
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
            
            if 'seed' in sys.modules:
                del sys.modules['seed']
            
            import seed
            
            calls = mock_randint.call_args_list
            assert any(call[0] == (1, 3) for call in calls)


class TestOutputAndLogging:
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
            
            if 'seed' in sys.modules:
                del sys.modules['seed']
            
            import seed
            
            assert mock_print.called
            print_args = [str(call[0][0]) for call in mock_print.call_args_list]
            has_success_msg = any('Seed done' in arg or 'Users total' in arg for arg in print_args)
            assert has_success_msg, "Success message not printed"
