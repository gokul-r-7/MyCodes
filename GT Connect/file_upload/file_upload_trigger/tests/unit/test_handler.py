# pylint: disable=redefined-outer-name,missing-docstring
import pytest
import sum
def test_lambda_handler():
    """This function test the lambda function code"""
    result = sum.sum_function(1,3)
    assert result == 4
