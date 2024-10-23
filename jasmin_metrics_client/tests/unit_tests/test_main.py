"""
Unit tests for the main package
"""

import unittest

from jasmin_metrics_client.main import main


class TestMain(unittest.TestCase):
    """
    Test the functionality of the main package.
    """

    def test_main(self) -> None:
        """
        Test the functionality of the :py:func:`jasmin_metrics_client.main` function
        """

        main()
