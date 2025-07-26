"""
Data Quality Validator for Olympic Analytics Platform.
Provides comprehensive data validation and quality checks.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import json

from ..utils.config import get_config
from ..utils.logger import DataQualityLogger


@dataclass
class ValidationRule:
    """Represents a data validation rule."""
    name: str
    description: str
    rule_type: str
    parameters: Dict[str, Any]
    severity: str = "error"  # error, warning, info


@dataclass
class ValidationResult:
    """Represents the result of a validation check."""
    rule_name: str
    passed: bool
    message: str
    details: Dict[str, Any]
    severity: str
    timestamp: datetime


class DataValidator:
    """Comprehensive data validator for Olympic data."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize data validator.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.dq_config = config.get('data_quality', {})
        self.logger = DataQualityLogger("data_validator", config.get('logging', {}))
        self.validation_rules = self._load_validation_rules()
    
    def _load_validation_rules(self) -> Dict[str, List[ValidationRule]]:
        """Load validation rules from configuration."""
        rules_config = self.dq_config.get('rules', {})
        rules = {}
        
        for dataset_name, dataset_rules in rules_config.items():
            rules[dataset_name] = []
            
            # Required fields rule
            if 'required_fields' in dataset_rules:
                rules[dataset_name].append(ValidationRule(
                    name="required_fields",
                    description="Check for required fields",
                    rule_type="required_fields",
                    parameters={"fields": dataset_rules['required_fields']},
                    severity="error"
                ))
            
            # Data types rule
            if 'data_types' in dataset_rules:
                rules[dataset_name].append(ValidationRule(
                    name="data_types",
                    description="Validate data types",
                    rule_type="data_types",
                    parameters={"types": dataset_rules['data_types']},
                    severity="error"
                ))
            
            # Constraints rule
            if 'constraints' in dataset_rules:
                rules[dataset_name].append(ValidationRule(
                    name="constraints",
                    description="Validate data constraints",
                    rule_type="constraints",
                    parameters={"constraints": dataset_rules['constraints']},
                    severity="warning"
                ))
        
        return rules
    
    def validate_dataset(self, dataset_name: str, data: pd.DataFrame) -> List[ValidationResult]:
        """
        Validate a dataset against configured rules.
        
        Args:
            dataset_name: Name of the dataset
            data: DataFrame to validate
            
        Returns:
            List of validation results
        """
        self.logger.validation_start(dataset_name=dataset_name, record_count=len(data))
        
        results = []
        dataset_rules = self.validation_rules.get(dataset_name, [])
        
        for rule in dataset_rules:
            try:
                result = self._apply_validation_rule(rule, data)
                results.append(result)
                
                if not result.passed and result.severity == "error":
                    self.logger.validation_failed(
                        rule_name=rule.name,
                        details=result.message,
                        dataset_name=dataset_name
                    )
                    
            except Exception as e:
                error_result = ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    message=f"Validation rule failed with error: {str(e)}",
                    details={"error": str(e)},
                    severity="error",
                    timestamp=datetime.utcnow()
                )
                results.append(error_result)
                self.logger.validation_failed(
                    rule_name=rule.name,
                    details=str(e),
                    dataset_name=dataset_name
                )
        
        # Calculate quality metrics
        quality_metrics = self._calculate_quality_metrics(results, data)
        self.logger.quality_metrics(quality_metrics, dataset_name=dataset_name)
        
        self.logger.validation_complete(
            results={"total_rules": len(results), "passed_rules": sum(1 for r in results if r.passed)},
            dataset_name=dataset_name
        )
        
        return results
    
    def _apply_validation_rule(self, rule: ValidationRule, data: pd.DataFrame) -> ValidationResult:
        """Apply a specific validation rule to the data."""
        if rule.rule_type == "required_fields":
            return self._validate_required_fields(rule, data)
        elif rule.rule_type == "data_types":
            return self._validate_data_types(rule, data)
        elif rule.rule_type == "constraints":
            return self._validate_constraints(rule, data)
        else:
            return ValidationResult(
                rule_name=rule.name,
                passed=False,
                message=f"Unknown rule type: {rule.rule_type}",
                details={},
                severity="error",
                timestamp=datetime.utcnow()
            )
    
    def _validate_required_fields(self, rule: ValidationRule, data: pd.DataFrame) -> ValidationResult:
        """Validate that required fields are present and not null."""
        required_fields = rule.parameters.get("fields", [])
        missing_fields = [field for field in required_fields if field not in data.columns]
        
        if missing_fields:
            return ValidationResult(
                rule_name=rule.name,
                passed=False,
                message=f"Missing required fields: {missing_fields}",
                details={"missing_fields": missing_fields},
                severity=rule.severity,
                timestamp=datetime.utcnow()
            )
        
        # Check for null values in required fields
        null_counts = {}
        for field in required_fields:
            null_count = data[field].isnull().sum()
            if null_count > 0:
                null_counts[field] = null_count
        
        if null_counts:
            return ValidationResult(
                rule_name=rule.name,
                passed=False,
                message=f"Null values found in required fields: {null_counts}",
                details={"null_counts": null_counts},
                severity=rule.severity,
                timestamp=datetime.utcnow()
            )
        
        return ValidationResult(
            rule_name=rule.name,
            passed=True,
            message="All required fields are present and non-null",
            details={"required_fields": required_fields},
            severity=rule.severity,
            timestamp=datetime.utcnow()
        )
    
    def _validate_data_types(self, rule: ValidationRule, data: pd.DataFrame) -> ValidationResult:
        """Validate data types of columns."""
        expected_types = rule.parameters.get("types", {})
        type_errors = {}
        
        for column, expected_type in expected_types.items():
            if column not in data.columns:
                type_errors[column] = f"Column not found"
                continue
            
            actual_type = str(data[column].dtype)
            
            # Simple type checking (can be enhanced)
            if expected_type == "string" and not actual_type.startswith("object"):
                type_errors[column] = f"Expected string, got {actual_type}"
            elif expected_type == "integer" and not actual_type.startswith("int"):
                type_errors[column] = f"Expected integer, got {actual_type}"
            elif expected_type == "float" and not actual_type.startswith("float"):
                type_errors[column] = f"Expected float, got {actual_type}"
        
        if type_errors:
            return ValidationResult(
                rule_name=rule.name,
                passed=False,
                message=f"Data type validation failed: {type_errors}",
                details={"type_errors": type_errors},
                severity=rule.severity,
                timestamp=datetime.utcnow()
            )
        
        return ValidationResult(
            rule_name=rule.name,
            passed=True,
            message="All data types are correct",
            details={"expected_types": expected_types},
            severity=rule.severity,
            timestamp=datetime.utcnow()
        )
    
    def _validate_constraints(self, rule: ValidationRule, data: pd.DataFrame) -> ValidationResult:
        """Validate data constraints."""
        constraints = rule.parameters.get("constraints", {})
        constraint_violations = {}
        
        for constraint_name, constraint_config in constraints.items():
            if constraint_name == "name_min_length":
                min_length = constraint_config
                short_names = data[data['name'].str.len() < min_length]
                if len(short_names) > 0:
                    constraint_violations[constraint_name] = f"{len(short_names)} names shorter than {min_length} characters"
            
            elif constraint_name == "country_valid_codes":
                # This would typically check against a list of valid country codes
                # For now, we'll just check for non-empty values
                invalid_countries = data[data['country'].isnull() | (data['country'] == '')]
                if len(invalid_countries) > 0:
                    constraint_violations[constraint_name] = f"{len(invalid_countries)} invalid country codes"
            
            elif constraint_name == "medal_counts_non_negative":
                medal_columns = ['gold', 'silver', 'bronze']
                for col in medal_columns:
                    if col in data.columns:
                        negative_medals = data[data[col] < 0]
                        if len(negative_medals) > 0:
                            constraint_violations[f"{col}_negative"] = f"{len(negative_medals)} negative {col} medal counts"
        
        if constraint_violations:
            return ValidationResult(
                rule_name=rule.name,
                passed=False,
                message=f"Constraint violations found: {constraint_violations}",
                details={"constraint_violations": constraint_violations},
                severity=rule.severity,
                timestamp=datetime.utcnow()
            )
        
        return ValidationResult(
            rule_name=rule.name,
            passed=True,
            message="All constraints satisfied",
            details={"constraints": list(constraints.keys())},
            severity=rule.severity,
            timestamp=datetime.utcnow()
        )
    
    def _calculate_quality_metrics(self, results: List[ValidationResult], data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate data quality metrics."""
        total_rules = len(results)
        passed_rules = sum(1 for r in results if r.passed)
        error_rules = sum(1 for r in results if not r.passed and r.severity == "error")
        warning_rules = sum(1 for r in results if not r.passed and r.severity == "warning")
        
        # Calculate completeness (percentage of non-null values)
        completeness = {}
        for column in data.columns:
            non_null_count = data[column].notnull().sum()
            completeness[column] = non_null_count / len(data) if len(data) > 0 else 0
        
        overall_completeness = np.mean(list(completeness.values())) if completeness else 0
        
        # Calculate accuracy (percentage of passed validation rules)
        accuracy = passed_rules / total_rules if total_rules > 0 else 0
        
        # Calculate consistency (based on data type consistency)
        consistency = 1.0  # Simplified for now
        
        return {
            "completeness": overall_completeness,
            "accuracy": accuracy,
            "consistency": consistency,
            "total_rules": total_rules,
            "passed_rules": passed_rules,
            "error_rules": error_rules,
            "warning_rules": warning_rules,
            "column_completeness": completeness
        }
    
    def validate_olympic_data(self, athletes_data: pd.DataFrame, medals_data: pd.DataFrame, 
                             teams_data: pd.DataFrame) -> Dict[str, List[ValidationResult]]:
        """
        Validate all Olympic datasets.
        
        Args:
            athletes_data: Athletes DataFrame
            medals_data: Medals DataFrame
            teams_data: Teams DataFrame
            
        Returns:
            Dictionary of validation results by dataset
        """
        results = {}
        
        # Validate each dataset
        if athletes_data is not None:
            results['athletes'] = self.validate_dataset('athletes', athletes_data)
        
        if medals_data is not None:
            results['medals'] = self.validate_dataset('medals', medals_data)
        
        if teams_data is not None:
            results['teams'] = self.validate_dataset('teams', teams_data)
        
        return results
    
    def generate_quality_report(self, validation_results: Dict[str, List[ValidationResult]]) -> Dict[str, Any]:
        """Generate a comprehensive quality report."""
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "datasets": {},
            "overall_quality": {}
        }
        
        total_rules = 0
        total_passed = 0
        total_errors = 0
        total_warnings = 0
        
        for dataset_name, results in validation_results.items():
            dataset_rules = len(results)
            dataset_passed = sum(1 for r in results if r.passed)
            dataset_errors = sum(1 for r in results if not r.passed and r.severity == "error")
            dataset_warnings = sum(1 for r in results if not r.passed and r.severity == "warning")
            
            report["datasets"][dataset_name] = {
                "total_rules": dataset_rules,
                "passed_rules": dataset_passed,
                "error_rules": dataset_errors,
                "warning_rules": dataset_warnings,
                "quality_score": dataset_passed / dataset_rules if dataset_rules > 0 else 0,
                "results": [{
                    "rule_name": r.rule_name,
                    "passed": r.passed,
                    "message": r.message,
                    "severity": r.severity
                } for r in results]
            }
            
            total_rules += dataset_rules
            total_passed += dataset_passed
            total_errors += dataset_errors
            total_warnings += total_warnings
        
        report["overall_quality"] = {
            "total_rules": total_rules,
            "passed_rules": total_passed,
            "error_rules": total_errors,
            "warning_rules": total_warnings,
            "overall_quality_score": total_passed / total_rules if total_rules > 0 else 0
        }
        
        return report 
