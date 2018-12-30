# ESB EV Charger Api

A collection of AWS Lamda functions to transform a KML file containing EV charger locations and build an API around it.

## The functions

1. Requests the latest KML, parses it and inserts it into a DynamoDB Table.
2. API Gateway method calls a function to scan this table and return the results.
