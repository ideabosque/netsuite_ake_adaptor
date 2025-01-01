#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import logging
import traceback
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError

from silvaengine_utility import Utility


class NetSuiteAkeAdaptor(object):
    def __init__(self, logger: logging.Logger, **setting: Dict[str, Any]):
        """
        Initialize the OpenAIFunctBase class.
        :param logger: Logger instance for logging errors and information.
        :param setting: Configuration setting for AWS credentials and region.
        """
        try:
            self.logger = logger
            self.setting = setting
            self.data_views = setting.get("data_views", [])
            self.schemas = {}
            self._initialize_aws_lambda_client()
        except (BotoCoreError, NoCredentialsError) as boto_error:
            self.logger.error(f"AWS Boto3 error: {boto_error}")
            raise boto_error
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e

    def _initialize_aws_lambda_client(self):
        """
        Initialize the AWS Lambda client using the provided credentials or default configuration.
        """
        region_name = self.setting.get("region_name")
        aws_access_key_id = self.setting.get("aws_access_key_id")
        aws_secret_access_key = self.setting.get("aws_secret_access_key")

        if region_name and aws_access_key_id and aws_secret_access_key:
            self.aws_lambda = boto3.client(
                "lambda",
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
        else:
            self.aws_lambda = boto3.client("lambda")

    def fetch_graphql_schema(self, function_name: str) -> Dict[str, Any]:
        if self.schemas.get(function_name) is None:
            self.schemas[function_name] = Utility.fetch_graphql_schema(
                self.logger,
                self.setting["endpoint_id"],
                function_name,
                aws_lambda=self.aws_lambda,
            )
        return self.schemas[function_name]

    def execute_graphql_query(
        self,
        function_name: str,
        operation_name: str,
        operation_type: str,
        variables: Dict[str, Any],
    ) -> Dict[str, Any]:
        schema = self.fetch_graphql_schema(function_name)
        return Utility.execute_graphql_query(
            self.logger,
            self.setting["endpoint_id"],
            function_name,
            Utility.generate_graphql_operation(operation_name, operation_type, schema),
            variables,
            aws_lambda=self.aws_lambda,
        )

    def get_data_view(
        self, data_view_name: str, **parameters: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        _data_views = list(
            filter(
                lambda data_view: data_view["data_view_name"] == data_view_name,
                self.data_views,
            )
        )
        if len(_data_views) == 0:
            return None

        suiteql = _data_views[0]["configuration"]["suiteql"]

        if parameters.get("filters") and len(parameters["filters"]) > 0:
            filters = [
                f"a.{filter['attribute']} {filter['operator']} {filter['value']}"
                for filter in parameters["filters"]
            ]
            filters = " AND ".join(filters)
            suiteql = f"SELECT a.* FROM ({suiteql}) a WHERE {filters}"
        variables = {"suiteql": suiteql}
        if parameters.get("limit"):
            variables["limit"] = parameters["limit"]
        if parameters.get("offset"):
            variables["offset"] = parameters["offset"]

        response = self.execute_graphql_query(
            "netsuite_graphql",
            "suiteqlResult",
            "Query",
            variables,
        )

        return {
            "results": response["suiteqlResult"]["items"],
            "count": response["suiteqlResult"]["count"],
            "has_more": response["suiteqlResult"]["hasMore"],
            "offset": response["suiteqlResult"]["offset"],
            "total": response["suiteqlResult"]["totalResults"],
        }
