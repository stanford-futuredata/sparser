# System API

The API to the system is a SQL-style query with a `SELECT` and `WHERE` clause. Users also specify the file format used to represent the data.

For example, in Python:

```python
# First argument is the data type. Second argument is the columns the user wants. Third argument is predicates.
p = Sparser(Sparser.JSON, ["id", "user.name", "user.id"], ["user.name == (Tom || Dick || Harry)", "user.retweet_count > 50"])

data = p.parse("document.json").retrieve()

# Returns data as an array of tuples. Type of each object is inferred.
record = data[0]

print record.__class__ # <type 'tuple'>

print record[0] # the id
print record[1] # the username
print record[2] # the user ID

# Optionally, can specify a marshaller class to parse data into a particular format (useful for, e.g., parsing things into Java objects in the JNI).
data = p.parse("document.json").marshal(PythonMarshaller).retrieve()

record = data[0]

print record.__class__ # <class __main__.PythonMarshaller at ...>
```
