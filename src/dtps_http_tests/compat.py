"""Compatibility module for Python 3.6+ support in tests."""

try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    # Python < 3.8: Use unittest.TestCase with pytest-asyncio
    # Tests will be run with pytest-asyncio which handles async tests
    import unittest
    
    class IsolatedAsyncioTestCase(unittest.TestCase):
        """
        Compatibility shim for IsolatedAsyncioTestCase.
        
        In Python 3.8+, this is imported from unittest.
        In Python 3.6/3.7, this is a shim that works with pytest-asyncio.
        """
        pass
