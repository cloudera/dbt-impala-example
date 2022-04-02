{% test length(model, column_name, len)  %}

with validation as (
	select {{ column_name }} as field
	from {{ model }}
),
validation_errors as (
	select field from validation
	where LENGTH(field) != {{ len }}
)

select *
from validation_errors

{% endtest  %}
