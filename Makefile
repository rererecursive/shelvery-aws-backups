.PHONY: tests

integration_tests:
	python3 -m unittest shelvery_tests.ebs_integration_test
	#python3 -m unittest shelvery_tests.redshift_integration_test
	#python3 -m unittest shelvery_tests.rds_integration_test

unit_tests:
	python3 -m unittest shelvery_tests.engine_test
	python3 -m unittest shelvery_tests.backup_test

tests:
	integration_tests
	unittest_tests
