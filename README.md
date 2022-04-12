# dbt_ethereum



## Development


Install sqlfluff:

- pip install dbt
- pip install 'dbt-spark[PyHive]'


Install sqlfluff:

- pip install sqlfluff
- pip install sqlfluff-templater-dbt


Try running integration tests for utils:

- cd integration_tests
- dbt seed
- dbt run --models ./models/utils
- dbt test