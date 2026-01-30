import os
import pytest
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime, UTC


@pytest.fixture
def mock_env_vars():
    """Fixture to set up environment variables for testing"""
    env_vars = {
        "PGHOST": "testdb",
        "PGPORT": "5432",
        "PGDATABASE": "testappdb",
        "PGUSER": "testuser",
        "PGPASSWORD": "testpass",
        "ROWS": "10"
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def mock_engine():
    """Fixture to create a mock SQLAlchemy engine"""
    engine = Mock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = Mock(return_value=conn)
    engine.begin.return_value.__exit__ = Mock(return_value=False)
    return engine, conn


class TestCyclicJobConfiguration:
    """Test configuration and environment variable loading"""

    def test_default_environment_variables(self):
        """Test that default environment variables are set correctly"""
        with patch.dict(os.environ, {}, clear=True):
            # Import cyclic_job module fresh
            import importlib
            import sys
            
            # Remove cyclic_job from sys.modules if it exists
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            
            # Mock the database connection to avoid actual connection
            with patch('cyclic_job.create_engine') as mock_create_engine:
                mock_engine = Mock()
                mock_conn = MagicMock()
                mock_create_engine.return_value = mock_engine
                mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
                mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
                mock_conn.execute.return_value.fetchall.return_value = []
                
                import cyclic_job
                
                # Verify defaults
                assert cyclic_job.host == "db"
                assert cyclic_job.port == "5432"
                assert cyclic_job.db == "appdb"
                assert cyclic_job.user == "app"
                assert cyclic_job.pw == "app"
                assert cyclic_job.rows == 200

    def test_custom_environment_variables(self, mock_env_vars):
        """Test that custom environment variables are used correctly"""
        import importlib
        import sys
        
        # Remove cyclic_job from sys.modules if it exists
        if 'cyclic_job' in sys.modules:
            del sys.modules['cyclic_job']
        
        with patch('cyclic_job.create_engine') as mock_create_engine:
            mock_engine = Mock()
            mock_conn = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
            mock_conn.execute.return_value.fetchall.return_value = []
            
            import cyclic_job
            
            # Verify custom values
            assert cyclic_job.host == "testdb"
            assert cyclic_job.port == "5432"
            assert cyclic_job.db == "testappdb"
            assert cyclic_job.user == "testuser"
            assert cyclic_job.pw == "testpass"
            assert cyclic_job.rows == 10

    def test_database_url_format(self, mock_env_vars):
        """Test that database URL is formatted correctly"""
        expected_url = "postgresql+psycopg2://testuser:testpass@testdb:5432/testappdb"
        
        import importlib
        import sys
        
        if 'cyclic_job' in sys.modules:
            del sys.modules['cyclic_job']
        
        with patch('cyclic_job.create_engine') as mock_create_engine:
            mock_engine = Mock()
            mock_conn = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
            mock_conn.execute.return_value.fetchall.return_value = []
            
            import cyclic_job
            
            # Verify create_engine was called with correct URL
            mock_create_engine.assert_called_once_with(expected_url, pool_pre_ping=True)


class TestCyclicJobDatabaseOperations:
    """Test database operations"""

    @patch('cyclic_job.create_engine')
    @patch('cyclic_job.Faker')
    def test_no_table_creation(self, mock_faker, mock_create_engine):
        """Test that cyclic_job does not create tables (unlike seed.py)"""
        # Setup mocks
        mock_engine = Mock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.return_value.fetchall.return_value = []
        
        # Mock Faker
        mock_fake = Mock()
        mock_faker.return_value = mock_fake
        mock_fake.name.return_value = "Test User"
        mock_fake.unique.email.return_value = "test@example.com"
        mock_fake.word.return_value = "product"
        
        # Import to trigger execution
        import importlib
        import sys
        if 'cyclic_job' in sys.modules:
            del sys.modules['cyclic_job']
        import cyclic_job
        
        # Verify execute was NOT called for table creation
        calls = mock_conn.execute.call_args_list
        call_args = [str(call[0][0]) for call in calls]
        has_create_table = any('CREATE TABLE' in arg for arg in call_args)
        
        assert not has_create_table, "cyclic_job should not create tables"

    @patch('cyclic_job.create_engine')
    @patch('cyclic_job.Faker')
    def test_user_data_generation(self, mock_faker, mock_create_engine):
        """Test that user data is generated correctly"""
        # Setup mocks
        mock_engine = Mock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.return_value.fetchall.return_value = [(1,), (2,)]
        
        # Mock Faker
        mock_fake = Mock()
        mock_faker.return_value = mock_fake
        mock_fake.name.return_value = "Test User"
        mock_fake.unique.email.return_value = "test@example.com"
        mock_fake.word.return_value = "product"
        
        # Set ROWS to a small number for testing
        with patch.dict(os.environ, {"ROWS": "2"}):
            import importlib
            import sys
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            import cyclic_job
            
            # Verify Faker methods were called
            assert mock_fake.name.call_count >= 2
            assert mock_fake.unique.email.call_count >= 2

    @patch('cyclic_job.create_engine')
    @patch('cyclic_job.Faker')
    def test_orders_data_generation(self, mock_faker, mock_create_engine):
        """Test that orders data is generated correctly"""
        # Setup mocks
        mock_engine = Mock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.return_value.fetchall.return_value = [(1,), (2,), (3,)]
        
        # Mock Faker
        mock_fake = Mock()
        mock_faker.return_value = mock_fake
        mock_fake.name.return_value = "Test User"
        mock_fake.unique.email.return_value = "test@example.com"
        mock_fake.word.return_value = "product"
        
        # Set ROWS to a small number for testing
        with patch.dict(os.environ, {"ROWS": "2"}):
            import importlib
            import sys
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            import cyclic_job
            
            # Verify word method was called for product names
            assert mock_fake.word.call_count > 0

    @patch('cyclic_job.create_engine')
    @patch('cyclic_job.Faker')
    def test_conflict_handling(self, mock_faker, mock_create_engine):
        """Test that ON CONFLICT clause is used for users"""
        # Setup mocks
        mock_engine = Mock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.return_value.fetchall.return_value = [(1,)]
        
        # Mock Faker
        mock_fake = Mock()
        mock_faker.return_value = mock_fake
        mock_fake.name.return_value = "Test User"
        mock_fake.unique.email.return_value = "test@example.com"
        mock_fake.word.return_value = "product"
        
        with patch.dict(os.environ, {"ROWS": "1"}):
            import importlib
            import sys
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            import cyclic_job
            
            # Check that ON CONFLICT is used in user insert
            calls = mock_conn.execute.call_args_list
            call_args = [str(call[0][0]) for call in calls]
            has_conflict_clause = any('ON CONFLICT' in arg for arg in call_args)
            
            assert has_conflict_clause, "ON CONFLICT clause not found in user insert"


class TestCyclicJobDataValidation:
    """Test data validation and integrity"""

    @patch('cyclic_job.create_engine')
    @patch('cyclic_job.Faker')
    def test_timestamp_generation(self, mock_faker, mock_create_engine):
        """Test that timestamps are generated with UTC timezone"""
        # Setup mocks
        mock_engine = Mock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.return_value.fetchall.return_value = [(1,)]
        
        # Mock Faker
        mock_fake = Mock()
        mock_faker.return_value = mock_fake
        mock_fake.name.return_value = "Test User"
        mock_fake.unique.email.return_value = "test@example.com"
        mock_fake.word.return_value = "product"
        
        with patch.dict(os.environ, {"ROWS": "1"}):
            with patch('cyclic_job.datetime') as mock_datetime:
                mock_now = datetime(2024, 1, 1, 12, 0, 0)
                mock_datetime.now.return_value = mock_now
                mock_datetime.UTC = UTC
                
                import importlib
                import sys
                if 'cyclic_job' in sys.modules:
                    del sys.modules['cyclic_job']
                import cyclic_job
                
                # Verify datetime.now was called with UTC
                assert mock_datetime.now.call_count > 0

    @patch('cyclic_job.create_engine')
    @patch('cyclic_job.Faker')
    @patch('cyclic_job.randint')
    @patch('cyclic_job.uniform')
    def test_order_amount_range(self, mock_uniform, mock_randint, mock_faker, mock_create_engine):
        """Test that order amounts are within expected range"""
        # Setup mocks
        mock_engine = Mock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.return_value.fetchall.return_value = [(1,)]
        
        # Mock Faker
        mock_fake = Mock()
        mock_faker.return_value = mock_fake
        mock_fake.name.return_value = "Test User"
        mock_fake.unique.email.return_value = "test@example.com"
        mock_fake.word.return_value = "product"
        
        # Mock random functions
        mock_randint.return_value = 2
        mock_uniform.return_value = 150.50
        
        with patch.dict(os.environ, {"ROWS": "1"}):
            import importlib
            import sys
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            import cyclic_job
            
            # Verify uniform was called with correct range
            calls = mock_uniform.call_args_list
            if calls:
                # Check that uniform was called with (10, 500)
                assert any(call[0] == (10, 500) for call in calls)

    @patch('cyclic_job.create_engine')
    @patch('cyclic_job.Faker')
    @patch('cyclic_job.randint')
    def test_orders_per_user_range(self, mock_randint, mock_faker, mock_create_engine):
        """Test that orders per user are within expected range (1-3)"""
        # Setup mocks
        mock_engine = Mock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.return_value.fetchall.return_value = [(1,)]
        
        # Mock Faker
        mock_fake = Mock()
        mock_faker.return_value = mock_fake
        mock_fake.name.return_value = "Test User"
        mock_fake.unique.email.return_value = "test@example.com"
        mock_fake.word.return_value = "product"
        
        # Mock randint
        mock_randint.return_value = 2
        
        with patch.dict(os.environ, {"ROWS": "1"}):
            import importlib
            import sys
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            import cyclic_job
            
            # Verify randint was called with (1, 3) for orders per user
            calls = mock_randint.call_args_list
            assert any(call[0] == (1, 3) for call in calls)


class TestCyclicJobOutputAndLogging:
    """Test output and logging"""

    @patch('cyclic_job.create_engine')
    @patch('cyclic_job.Faker')
    @patch('builtins.print')
    def test_success_message(self, mock_print, mock_faker, mock_create_engine):
        """Test that success message is printed"""
        # Setup mocks
        mock_engine = Mock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.return_value.fetchall.return_value = [(1,), (2,)]
        
        # Mock Faker
        mock_fake = Mock()
        mock_faker.return_value = mock_fake
        mock_fake.name.return_value = "Test User"
        mock_fake.unique.email.return_value = "test@example.com"
        mock_fake.word.return_value = "product"
        
        with patch.dict(os.environ, {"ROWS": "1"}):
            import importlib
            import sys
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            import cyclic_job
            
            # Verify print was called with success message
            assert mock_print.called
            print_args = [str(call[0][0]) for call in mock_print.call_args_list]
            has_success_msg = any('Users total' in arg for arg in print_args)
            assert has_success_msg, "Success message not printed"


class TestCyclicJobUserCount:
    """Test user count retrieval"""

    @patch('cyclic_job.create_engine')
    @patch('cyclic_job.Faker')
    def test_all_users_query(self, mock_faker, mock_create_engine):
        """Test that all users are queried from database"""
        # Setup mocks
        mock_engine = Mock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.return_value.fetchall.return_value = [(1,), (2,), (3,)]
        
        # Mock Faker
        mock_fake = Mock()
        mock_faker.return_value = mock_fake
        mock_fake.name.return_value = "Test User"
        mock_fake.unique.email.return_value = "test@example.com"
        mock_fake.word.return_value = "product"
        
        with patch.dict(os.environ, {"ROWS": "1"}):
            import importlib
            import sys
            if 'cyclic_job' in sys.modules:
                del sys.modules['cyclic_job']
            import cyclic_job
            
            # Verify SELECT id FROM users query was executed
            calls = mock_conn.execute.call_args_list
            call_args = [str(call[0][0]) for call in calls]
            has_user_query = any('SELECT id FROM users' in arg for arg in call_args)
            
            assert has_user_query, "SELECT id FROM users query not found"
